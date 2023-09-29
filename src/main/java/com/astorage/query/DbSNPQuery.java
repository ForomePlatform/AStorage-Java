package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.dbsnp.DbSNPConstants;
import com.astorage.utils.dbsnp.DbSNPHelper;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class DbSNPQuery extends SingleFormatQuery implements Constants, DbSNPConstants {
	public DbSNPQuery(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void queryHandler() throws IOException {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 2
				|| !req.params().contains(CHR_PARAM)
				|| !req.params().contains(POS_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String chr = req.getParam(CHR_PARAM);
		String pos = req.getParam(POS_PARAM);

		singleQueryHandler(chr, pos, false);
	}

	protected void singleQueryHandler(String chr, String pos, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();
		JsonObject errorJson = new JsonObject();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			errorJson.put(ERROR, INVALID_CHR_OR_POS_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		byte[] key = DbSNPHelper.createKey(chr, pos);
		byte[] compressedVariants = dbRep.getBytes(key);

		if (compressedVariants == null) {
			errorJson.put(ERROR, VARIANT_NOT_FOUND_ERROR);

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

		String variantsString = Constants.decompressJson(compressedVariants);
		JsonArray variantsJson = new JsonArray(variantsString);

		result.put(VARIANTS_KEY, variantsJson);

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "text/json")
				.end(result + "\n");
		}
	}
}
