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
public class DbNSFPQuery extends SingleFormatQuery implements Constants, DbNSFPConstants {
	public DbNSFPQuery(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void queryHandler() throws IOException {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 2 && (req.params().size() != 3 || !req.params().contains(ALT_PARAM))
				|| !req.params().contains(CHR_PARAM)
				|| !req.params().contains(POS_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String chr = req.getParam(CHR_PARAM);
		String pos = req.getParam(POS_PARAM);
		String alt = req.params().contains(ALT_PARAM) ? req.getParam(ALT_PARAM) : null;

		singleQueryHandler(chr, pos, alt, false);
	}

	protected void singleQueryHandler(String chr, String pos, String alt, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_CHR_OR_POS_ERROR);

			return;
		}

		if (alt != null && (alt.length() != 1 || !NUCLEOTIDES.contains(alt.toUpperCase()))) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_ALT_ERROR);

			return;
		}

		try {
			JsonObject result = queryData(dbRep, chr, pos, alt);

			if (isBatched) {
				req.response().write(result + "\n");
			} else {
				req.response()
					.putHeader("content-type", "application/json")
					.end(result + "\n");
			}
		} catch (Exception e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
		}
	}

	public static JsonObject queryData(RocksDBRepository dbRep, String chr, String pos, String alt) throws Exception {
		chr = chr.toUpperCase();

		byte[] key = DbNSFPHelper.createKey(chr, pos);
		byte[] compressedVariants = dbRep.getBytes(key);
		if (compressedVariants == null) {
			throw new Exception(VARIANT_NOT_FOUND_ERROR);
		}

		String decompressedVariants = Constants.decompressJson(compressedVariants);
		JsonArray variants = DbNSFPHelper.processRawVariantsJson(new JsonArray(decompressedVariants));

		JsonObject result = new JsonObject();
		result.put(CHR_PARAM, chr);
		result.put(POS_PARAM, pos);

		if (alt != null) {
			alt = alt.toUpperCase();
			result.put(ALT_PARAM, alt);

			JsonArray filteredVariants = new JsonArray();

			for (Object variantObject : variants) {
				JsonObject variant = (JsonObject) variantObject;
				String variantAlt = variant.getString(Variant.VARIANT_ALT);

				if (variantAlt.equalsIgnoreCase(alt)) {
					filteredVariants.add(variant);
				}
			}

			result.put(VARIANTS_KEY, filteredVariants);
		} else {
			result.put(VARIANTS_KEY, variants);
		}

		return result;
	}

	public static String[] normalizedParamsToParams(
		String refBuild,
		String chr,
		String pos,
		String ref,
		String alt
	) {
		return new String[]{chr, pos, alt};
	}
}
