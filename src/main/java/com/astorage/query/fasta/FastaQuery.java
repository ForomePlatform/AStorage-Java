package com.astorage.query.fasta;

import com.astorage.db.RocksDBRepository;
import com.astorage.ingestion.FastaIngestor;
import com.astorage.utils.Constants;
import com.astorage.query.Query;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.net.HttpURLConnection;

public class FastaQuery implements Query, Constants {
	private final RoutingContext context;
	private final RocksDBRepository dbRep;

	public FastaQuery(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void queryHandler() {
		HttpServerRequest req = context.request();
		if (!(req.params().size() == 4
			&& req.params().contains("arrayName")
			&& req.params().contains("sectionName")
			&& req.params().contains("startPosition")
			&& req.params().contains("endPosition"))) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, ERROR_INVALID_PARAMS);
			return;
		}

		String arrayName = req.getParam("arrayName");
		String sectionName = req.getParam("sectionName");
		int startPosition = Integer.parseInt(req.getParam("startPosition"));
		int endPosition = Integer.parseInt(req.getParam("endPosition"));

		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(arrayName);

		if (columnFamilyHandle == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, COLUMN_FAMILY_NULL);
			return;
		}

		StringBuilder stringBuilder = new StringBuilder();
		for (int i = startPosition; i <= endPosition; i++) {
			stringBuilder.append(dbRep.find(FastaIngestor.generateDBKey(sectionName, i), columnFamilyHandle));
		}

		req.response()
			.putHeader("content-type", "text/json")
			.end(
				new JsonObject()
					.put("array", arrayName)
					.put("section", sectionName)
					.put("start", startPosition)
					.put("end", endPosition)
					.put("data", stringBuilder.toString())
					.toString() + "\n"
			);
	}
}
