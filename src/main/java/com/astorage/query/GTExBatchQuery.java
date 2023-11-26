package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.gtex.GTExConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class GTExBatchQuery extends GTExQuery implements Constants, GTExConstants {
	public GTExBatchQuery(RoutingContext context, RocksDBRepository dbRep) {
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

					String dataType = query.getString(DATA_TYPE_PARAM);

					if (dataType != null) {
						if (dataType.equals(GENE_COLUMN_FAMILY_NAME)) {
							String geneId = query.getString(GENE_ID_PARAM);
							String subId = query.getString(SUB_ID_PARAM);

							singleQueryHandlerForGene(geneId, subId, true);
						}

						if (dataType.equals(TISSUE_COLUMN_FAMILY_NAME)) {
							String tissueNo = query.getString(TISSUE_NUMBER_PARAM);

							singleQueryHandlerForTissue(tissueNo, true);
						}

						if (dataType.equals(GENE_TO_TISSUE_COLUMN_FAMILY_NAME)) {
							String geneId = query.getString(GENE_ID_PARAM);
							String subId = query.getString(SUB_ID_PARAM);
							String tissueNo = query.getString(TISSUE_NUMBER_PARAM);

							singleQueryHandlerForGeneToTissue(geneId, subId, tissueNo, true);
						}
					}
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
