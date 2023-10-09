package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.spliceai.SpliceAIConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.net.HttpURLConnection;

import static com.astorage.utils.spliceai.SpliceAIHelper.createKey;

@SuppressWarnings("unused")
public class SpliceAIQuery extends SingleFormatQuery implements Constants, SpliceAIConstants {
	public SpliceAIQuery(RoutingContext context, RocksDBRepository dbRep) {
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

		try {
			JsonObject result = queryData(dbRep, chr, pos);

			if (isBatched) {
				req.response().write(result + "\n");
			} else {
				req.response()
					.putHeader("content-type", "text/json")
					.end(result + "\n");
			}
		} catch (Exception e) {
			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				e.getMessage()
			);
		}
	}

	public static JsonObject queryData(RocksDBRepository dbRep, String chr, String pos) throws Exception {
		JsonObject errorJson = new JsonObject();

		byte[] key = createKey(chr, pos);

		byte[] compressedVariant = dbRep.getBytes(key);
		if (compressedVariant == null) {
			errorJson.put(ERROR, VARIANT_NOT_FOUND_ERROR);

			throw new Exception(errorJson.toString());
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);

        return new JsonObject(decompressedVariant);
	}
}
