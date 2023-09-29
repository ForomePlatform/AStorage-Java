package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.clinvar.Variant;
import com.astorage.utils.gtf.GTFConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.net.HttpURLConnection;

public class GTFQuery extends SingleFormatQuery implements Constants, GTFConstants {
	public GTFQuery(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void queryHandler() throws IOException {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 3
				|| !req.params().contains(CHR_PARAM)
				|| !req.params().contains(START_POS_PARAM)
				|| !req.params().contains(END_POS_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String chr = req.getParam(CHR_PARAM);
		String startPos = req.getParam(START_POS_PARAM);
		String endPos = req.getParam(END_POS_PARAM);

		singleQueryHandler(chr, startPos, endPos, false);
	}

	protected void singleQueryHandler(String chr, String startPos, String endPos, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();
		JsonObject errorJson = new JsonObject();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(startPos);
			Long.parseLong(endPos);
		} catch (NumberFormatException e) {
			errorJson.put(ERROR, INVALID_CHR_OR_POS_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		byte[] compressedVariant = dbRep.getBytes(Variant.generateKey(chr, startPos, endPos));
		if (compressedVariant == null) {
			errorJson.put(ERROR, VARIANT_NOT_FOUND_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);
		JsonObject result = new JsonObject(decompressedVariant);

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "text/json")
				.end(result + "\n");
		}
	}
}
