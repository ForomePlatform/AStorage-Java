package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.clinvar.ClinVarConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class ClinVarBatchQuery extends ClinVarQuery implements Constants, ClinVarConstants {
	public ClinVarBatchQuery(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void queryHandler() {
		HttpServerRequest req = context.request();

		req.bodyHandler(buffer -> {
			try {
				JsonArray queries = buffer.toJsonArray();

				req.response().setChunked(true);
				req.response().putHeader("content-type", "application/octet-stream");

				for (Object queryObject : queries) {
					JsonObject query = (JsonObject) queryObject;

					String chr = query.getString(CHR_PARAM);
					String startPos = query.getString(START_POS_PARAM);
					String endPos = query.getString(END_POS_PARAM);

					singleQueryHandler(chr, startPos, endPos, true);
				}

				req.response().end();
			} catch (DecodeException e) {
				Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, JSON_DECODE_ERROR);
			} catch (IOException e) {
				Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
			}
		});
	}
}
