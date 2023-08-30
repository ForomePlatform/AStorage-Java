package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.fasta.FastaConstants;
import com.astorage.utils.fasta.FastaHelper;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

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
			req.params().size() != 4
				|| !req.params().contains(ARRAY_NAME_PARAM)
				|| !req.params().contains(SECTION_NAME_PARAM)
				|| !req.params().contains(START_POS_PARAM)
				|| !req.params().contains(END_POS_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String arrayName = req.getParam(ARRAY_NAME_PARAM);
		String sectionName = req.getParam(SECTION_NAME_PARAM);
		int startPosition = Integer.parseInt(req.getParam(START_POS_PARAM));
		int endPosition = Integer.parseInt(req.getParam(END_POS_PARAM));

		singleQueryHandler(arrayName, sectionName, startPosition, endPosition, false);
	}

	public void singleQueryHandler(String arrayName, String sectionName, int startPos, int endPos, boolean isBatched) {
		HttpServerRequest req = context.request();

		String data;
		try {
			data = FastaHelper.queryData(dbRep, arrayName, sectionName, startPos, endPos);
		} catch (InternalError e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());

			return;
		}

		String result = new JsonObject().put("array", arrayName)
			.put("section", sectionName)
			.put("start", startPos)
			.put("end", endPos)
			.put("data", data)
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
