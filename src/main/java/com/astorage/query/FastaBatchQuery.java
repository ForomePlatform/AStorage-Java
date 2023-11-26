package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.fasta.FastaConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class FastaBatchQuery extends FastaQuery implements Constants, FastaConstants {
	public FastaBatchQuery(RoutingContext context, RocksDBRepository dbRep) {
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

					String refBuild = query.getString(REF_BUILD_PARAM);
					String chr = query.getString(CHR_PARAM);
					String startPosition = query.getString(START_POS_PARAM);
					String endPosition = query.getString(END_POS_PARAM);

					singleQueryHandler(refBuild, chr, startPosition, endPosition, true);
				}

				req.response().end();
			} catch (DecodeException e) {
				Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, JSON_DECODE_ERROR);
			}
		});
	}
}
