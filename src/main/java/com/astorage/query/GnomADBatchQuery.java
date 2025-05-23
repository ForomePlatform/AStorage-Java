package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.gnomad.GnomADConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class GnomADBatchQuery extends GnomADQuery implements Constants, GnomADConstants {
	public GnomADBatchQuery(RoutingContext context, RocksDBRepository dbRep) {
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
					String pos = query.getString(POS_PARAM);
					String sourceType = query.getString(SOURCE_TYPE_PARAM);

					singleQueryHandler(chr, pos, sourceType, true);
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
