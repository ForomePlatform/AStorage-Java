package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.pharmgkb.PharmGKBConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class PharmGKBQuery extends SingleFormatQuery implements Constants, PharmGKBConstants {
	public PharmGKBQuery(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void queryHandler() throws IOException {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 2
				|| !req.params().contains(DATA_TYPE_PARAM)
				|| !req.params().contains(ID_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String dataType = req.getParam(DATA_TYPE_PARAM);
		String id = req.getParam(ID_PARAM);

		singleQueryHandler(dataType, id,false);
	}

	protected void singleQueryHandler(String dataType, String id, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();
		JsonObject errorJson = new JsonObject();

		if (!(DATA_TYPES.contains(dataType))) {
			errorJson.put(ERROR, INVALID_DATA_TYPE_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(dataType);
		if (columnFamilyHandle == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, COLUMN_FAMILY_NULL_ERROR);
			return;
		}

		byte[] compressedVariant = dbRep.getBytes(id.getBytes(), columnFamilyHandle);
		if (compressedVariant == null) {
			errorJson.put(ERROR, VARIANT_NOT_FOUND_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);
		JsonObject result = new JsonObject(decompressedVariant);
		result.put(DATA_TYPE_FIELD_NAME, dataType);

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "text/json")
				.end(result + "\n");
		}
	}
}
