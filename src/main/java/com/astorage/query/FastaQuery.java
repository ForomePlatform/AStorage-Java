package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.ingestion.FastaIngestor;
import com.astorage.utils.Constants;
import com.astorage.utils.fasta.FastaConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class FastaQuery implements Query, Constants, FastaConstants {
	protected final RoutingContext context;
	protected final RocksDBRepository dbRep;

	public FastaQuery(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void queryHandler() {
		HttpServerRequest req = context.request();

		if (
			req.params().size() == 4
				&& req.params().contains(ARRAY_NAME_PARAM)
				&& req.params().contains(SECTION_NAME_PARAM)
				&& req.params().contains(START_POS_PARAM)
				&& req.params().contains(END_POS_PARAM)
		) {
			String arrayName = req.getParam(ARRAY_NAME_PARAM);
			String sectionName = req.getParam(SECTION_NAME_PARAM);
			int startPosition = Integer.parseInt(req.getParam(START_POS_PARAM));
			int endPosition = Integer.parseInt(req.getParam(END_POS_PARAM));

			singleQueryHandler(arrayName, sectionName, startPosition, endPosition, false);

			return;
		}

		Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);
	}

	public void singleQueryHandler(String arrayName, String sectionName, int startPosition, int endPosition, boolean isBatched) {
		HttpServerRequest req = context.request();
		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(arrayName);

		if (columnFamilyHandle == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, COLUMN_FAMILY_NULL_ERROR);
			return;
		}

		StringBuilder stringBuilder = new StringBuilder();
		for (int i = startPosition; i <= endPosition; i++) {
			stringBuilder.append(dbRep.getString(FastaIngestor.generateDBKey(sectionName, i), columnFamilyHandle));
		}

		String result = new JsonObject().put("array", arrayName)
			.put("section", sectionName)
			.put("start", startPosition)
			.put("end", endPosition)
			.put("data", stringBuilder.toString())
			.toString();

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "text/json")
				.end(result + "\n");
		}
	}
}
