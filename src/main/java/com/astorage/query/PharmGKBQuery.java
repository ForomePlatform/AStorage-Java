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

		singleQueryHandler(dataType, id, false);
	}

	protected void singleQueryHandler(String dataType, String id, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();

		if (!(DATA_TYPES.contains(dataType))) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_DATA_TYPE_ERROR);

			return;
		}

		try {
			JsonObject result = queryData(dbRep, dataType, id);

			if (isBatched) {
				req.response().write(result + "\n");
			} else {
				req.response()
					.putHeader("content-type", "application/json")
					.end(result + "\n");
			}
		} catch (Exception e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
		}
	}

	public static JsonObject queryData(RocksDBRepository dbRep, String dataType, String id) throws Exception {
		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(dataType);
		if (columnFamilyHandle == null) {
			throw new Exception(COLUMN_FAMILY_NULL_ERROR);
		}

		byte[] compressedVariant = dbRep.getBytes(id.getBytes(), columnFamilyHandle);
		if (compressedVariant == null) {
			throw new Exception(VARIANT_NOT_FOUND_ERROR);
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);
		JsonObject result = new JsonObject(decompressedVariant);
		result.put(DATA_TYPE_FIELD_NAME, dataType);

		return result;
	}
}
