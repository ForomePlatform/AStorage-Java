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

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(startPos);
			Long.parseLong(endPos);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_CHR_OR_POS_ERROR);

			return;
		}

		try {
			JsonObject result = queryData(dbRep, chr, startPos, endPos);

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

	public static JsonObject queryData(RocksDBRepository dbRep, String chr, String startPos, String endPos) throws Exception {
		byte[] compressedVariant = dbRep.getBytes(Variant.generateKey(chr, startPos, endPos));
		if (compressedVariant == null) {
			throw new Exception(VARIANT_NOT_FOUND_ERROR);
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);

		return new JsonObject(decompressedVariant);
	}
}
