package com.astorage.normalization;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.net.HttpURLConnection;

public class VariantBatchNormalizer extends VariantNormalizer implements Normalizer {
	public VariantBatchNormalizer(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void normalizationHandler() {
		batchNormalizationHandler();
	}

	private void batchNormalizationHandler() {
		HttpServerRequest req = context.request();

		req.bodyHandler(buffer -> {
			try {
				JsonArray queries = buffer.toJsonArray();

				req.response().setChunked(true);
				req.response().putHeader("content-type", "text/json");

				for (Object queryObject : queries) {
					JsonObject query = (JsonObject) queryObject;

					String refBuild = query.getString(REF_BUILD_PARAM);
					String chr = query.getString(CHR_PARAM);
					String pos = query.getString(POS_PARAM);
					String ref = query.getString(REF_PARAM);
					String alt = query.getString(ALT_PARAM);

					singleNormalizationHandler(refBuild, chr, pos, ref, alt, true);
				}

				req.response().end();
			} catch (DecodeException e) {
				Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, JSON_DECODE_ERROR);
			}
		});
	}
}
