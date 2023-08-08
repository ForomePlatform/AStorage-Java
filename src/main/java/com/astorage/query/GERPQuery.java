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
public class GERPQuery implements Query, Constants, GERPConstants {
	protected final RoutingContext context;
	protected final RocksDBRepository dbRep;

	public GERPQuery(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
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
			errorJson.put("error", INVALID_CHR_OR_POS_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		byte[] compressedVariant = dbRep.getBytes(createKey(chr, pos));
		if (compressedVariant == null) {
			errorJson.put("error", VARIANT_NOT_FOUND_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);
		JsonArray result = new JsonArray(decompressedVariant);

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "text/json")
				.end(result + "\n");
		}
	}
}
