package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.gerp.GERPConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.net.HttpURLConnection;

import static com.astorage.utils.gerp.GERPHelper.createKey;

@SuppressWarnings("unused")
public class GERPQuery extends SingleFormatQuery implements Constants, GERPConstants {
	public GERPQuery(RoutingContext context, RocksDBRepository dbRep) {
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
		chr = chr.toUpperCase();

		byte[] compressedVariant = dbRep.getBytes(createKey(chr, pos));
		if (compressedVariant == null) {
			throw new Exception(VARIANT_NOT_FOUND_ERROR);
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);

		JsonObject result = new JsonObject();
		result.put("values", new JsonArray(decompressedVariant));

		return result;
	}

	public static String[] normalizedParamsToParams(
		String refBuild,
		String chr,
		String pos,
		String ref,
		String alt
	) {
		return new String[]{chr, pos};
	}
}
