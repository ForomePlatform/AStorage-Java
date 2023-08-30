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
		String startPos = req.getParam(START_POS_PARAM);
		String endPos = req.getParam(END_POS_PARAM);

		singleQueryHandler(arrayName, sectionName, startPos, endPos, false);
	}

	public void singleQueryHandler(String arrayName, String sectionName, String startPos, String endPos, boolean isBatched) {
		HttpServerRequest req = context.request();
		JsonObject errorJson = new JsonObject();

		try {
			Long.parseLong(startPos);
			Long.parseLong(endPos);
		} catch (NumberFormatException e) {
			errorJson.put(ERROR, INVALID_START_OR_END_POS_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		String data;
		try {
			data = FastaHelper.queryData(
				dbRep,
				arrayName,
				sectionName,
				Long.parseLong(startPos),
				Long.parseLong(endPos)
			);
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
