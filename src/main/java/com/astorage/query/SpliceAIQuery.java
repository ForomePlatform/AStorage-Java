package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.spliceai.SpliceAIConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
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

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_CHR_OR_POS_ERROR);

			return;
		}

		try {
			JsonObject result = queryData(dbRep, chr, pos);

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

	public static JsonObject queryData(RocksDBRepository dbRep, String chr, String pos) throws Exception {
		byte[] key = createKey(chr, pos);

		byte[] compressedVariants = dbRep.getBytes(key);
		if (compressedVariants == null) {
			throw new Exception(VARIANTS_NOT_FOUND_ERROR);
		}

		String decompressedVariants = Constants.decompressJson(compressedVariants);
		JsonArray variantsJsonArray = new JsonArray(decompressedVariants);
		JsonObject variantsJsonObject = new JsonObject();
		variantsJsonObject.put(QUERY_VARIANTS_KEY, variantsJsonArray);

		return variantsJsonObject;
	}
}
