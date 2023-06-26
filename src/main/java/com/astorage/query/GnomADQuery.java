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
public class GnomADQuery implements Query, Constants, GnomADConstants {
	protected final RoutingContext context;
	protected final RocksDBRepository dbRep;

	public GnomADQuery(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void queryHandler() throws IOException {
		HttpServerRequest req = context.request();

		if (req.params().size() == 3
			&& req.params().contains(CHR_PARAM)
			&& req.params().contains(POS_PARAM)
			&& req.params().contains(SOURCE_TYPE_PARAM)) {
			String chr = req.getParam(CHR_PARAM);
			String pos = req.getParam(POS_PARAM);
			String sourceType = req.getParam(SOURCE_TYPE_PARAM);

			singleQueryHandler(chr, pos, sourceType, false);

			return;
		}

		Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);
	}

	protected void singleQueryHandler(String chr, String pos, String sourceType, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();
		JsonObject errorJson = new JsonObject();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			errorJson.put("error", INVALID_CHR_OR_POS_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		if (!(sourceType.length() == 1 && SOURCE_TYPES.contains(sourceType.toLowerCase()))) {
			errorJson.put("error", INVALID_SOURCE_TYPE_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		byte[] key = GnomADHelper.createKey(chr, pos);

		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(sourceType);
		if (columnFamilyHandle == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, COLUMN_FAMILY_NULL_ERROR);
			return;
		}

		byte[] compressedVariant = dbRep.getBytes(key, columnFamilyHandle);
		if (compressedVariant == null) {
			errorJson.put("error", VARIANT_NOT_FOUND_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);
		JsonObject result = new JsonObject(decompressedVariant);
		result.put(SOURCE_TYPE_FIELD_NAME, sourceType);

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "text/json")
				.end(result + "\n");
		}
	}
}
