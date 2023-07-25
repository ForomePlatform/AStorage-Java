package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.dbnsfp.DbNSFPConstants;
import com.astorage.utils.dbnsfp.DbNSFPHelper;
import com.astorage.utils.dbnsfp.Variant;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class DbNSFPQuery implements Query, Constants, DbNSFPConstants {
	protected final RoutingContext context;
	protected final RocksDBRepository dbRep;

	public DbNSFPQuery(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void queryHandler() throws IOException {
		HttpServerRequest req = context.request();

		if (
			(req.params().size() == 2 || req.params().size() == 3 && req.params().contains(ALT_PARAM))
				&& req.params().contains(CHR_PARAM)
				&& req.params().contains(POS_PARAM)
		) {
			String chr = req.getParam(CHR_PARAM);
			String pos = req.getParam(POS_PARAM);
			String alt = req.params().contains(ALT_PARAM) ? req.getParam(ALT_PARAM).toUpperCase() : null;

			singleQueryHandler(chr, pos, alt, false);

			return;
		}

		Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);
	}

	protected void singleQueryHandler(String chr, String pos, String alt, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();
		JsonObject errorJson = new JsonObject();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			errorJson.put("error", INVALID_CHR_OR_POS_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		if (alt != null && (alt.length() != 1 || !NUCLEOTIDES.contains(alt))) {
			errorJson.put("error", INVALID_ALT_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		byte[] key = DbNSFPHelper.createKey(chr, pos);
		byte[] compressedVariants = dbRep.getBytes(key);

		if (compressedVariants == null) {
			errorJson.put("error", VARIANT_NOT_FOUND_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		JsonObject result = new JsonObject();
		result.put(CHR_PARAM, chr);
		result.put(POS_PARAM, pos);
		if (alt != null) {
			result.put(ALT_PARAM, alt);
		}

		String variantsString = Constants.decompressJson(compressedVariants);
		JsonArray variantsJson = DbNSFPHelper.processRawVariantsJson(new JsonArray(variantsString));

		if (alt != null) {
			JsonArray selectedVariantJson = new JsonArray();

			for (int i = 0; i < variantsJson.size(); i++) {
				JsonObject variantJson = variantsJson.getJsonObject(i);
				String nucleotide = variantJson.getString(Variant.VARIANT_ALT);

				if (nucleotide.equals(alt)) {
					selectedVariantJson.add(variantJson);
					result.put(VARIANTS_KEY, selectedVariantJson);
					break;
				}
			}
		} else {
			result.put(VARIANTS_KEY, variantsJson);
		}

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "text/json")
				.end(result + "\n");
		}
	}
}
