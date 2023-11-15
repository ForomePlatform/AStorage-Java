package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.gnomad.GnomADConstants;
import com.astorage.utils.gnomad.GnomADHelper;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class GnomADQuery extends SingleFormatQuery implements Constants, GnomADConstants {
	public GnomADQuery(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void queryHandler() throws IOException {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 3
				|| !req.params().contains(CHR_PARAM)
				|| !req.params().contains(POS_PARAM)
				|| !req.params().contains(SOURCE_TYPE_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String chr = req.getParam(CHR_PARAM);
		String pos = req.getParam(POS_PARAM);
		String sourceType = req.getParam(SOURCE_TYPE_PARAM);

		singleQueryHandler(chr, pos, sourceType, false);
	}

	protected void singleQueryHandler(String chr, String pos, String sourceType, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_CHR_OR_POS_ERROR);

			return;
		}

		if (!(sourceType.length() == 1 && SOURCE_TYPES.contains(sourceType.toLowerCase()))) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_SOURCE_TYPE_ERROR);

			return;
		}

		try {
			JsonObject result = queryData(dbRep, chr, pos, sourceType);

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

	public static JsonObject queryData(RocksDBRepository dbRep, String chr, String pos, String sourceType) throws Exception {
		byte[] key = GnomADHelper.createKey(chr, pos);

		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(sourceType);
		if (columnFamilyHandle == null) {
			throw new Exception(COLUMN_FAMILY_NULL_ERROR);
		}

		byte[] compressedVariant = dbRep.getBytes(key, columnFamilyHandle);
		if (compressedVariant == null) {
			throw new Exception(VARIANT_NOT_FOUND_ERROR);
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);
		JsonObject result = new JsonObject(decompressedVariant);
		result.put(SOURCE_TYPE_FIELD_NAME, sourceType);

		return result;
	}
}
