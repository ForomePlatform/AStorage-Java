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
public class FastaQuery extends SingleFormatQuery implements Constants, FastaConstants {
	public FastaQuery(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void queryHandler() {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 4
				|| !req.params().contains(REF_BUILD_PARAM)
				|| !req.params().contains(CHR_PARAM)
				|| !req.params().contains(START_POS_PARAM)
				|| !req.params().contains(END_POS_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String refBuild = req.getParam(REF_BUILD_PARAM);
		String chr = req.getParam(CHR_PARAM);
		String startPos = req.getParam(START_POS_PARAM);
		String endPos = req.getParam(END_POS_PARAM);

		singleQueryHandler(refBuild, chr, startPos, endPos, false);
	}

	public void singleQueryHandler(String refBuild, String chr, String startPos, String endPos, boolean isBatched) {
		HttpServerRequest req = context.request();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(startPos);
			Long.parseLong(endPos);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_CHR_START_POS_OR_END_POS_ERROR);

			return;
		}

		String data;
		try {
			data = queryData(
				dbRep,
				refBuild,
				chr,
				Long.parseLong(startPos),
				Long.parseLong(endPos)
			);
		} catch (InternalError e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());

			return;
		}

		JsonObject result = new JsonObject()
			.put("refBuild", refBuild)
			.put("chr", chr)
			.put("start", startPos)
			.put("end", endPos)
			.put("data", data);

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "application/json")
				.end(result + "\n");
		}
	}

	public static String queryData(
		RocksDBRepository dbRep,
		String refBuild,
		String chr,
		long startPos,
		long endPos
	) throws InternalError {
		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(refBuild);

		if (columnFamilyHandle == null) {
			throw new InternalError(COLUMN_FAMILY_NULL_ERROR);
		}

		StringBuilder data = new StringBuilder();
		for (long i = startPos; i <= endPos; i++) {
			String retrievedData = dbRep.getString(FastaIngestor.generateKey(chr, i), columnFamilyHandle);

			if (retrievedData == null) {
				return null;
			}

			data.append(retrievedData);
		}

		return data.toString();
	}

	public static String queryData(
		RocksDBRepository dbRep,
		String refBuild,
		String chr,
		long pos
	) {
		String retrievedData = queryData(dbRep, refBuild, chr, pos, pos);

		if (retrievedData == null) {
			return null;
		}

		return Character.toString(retrievedData.charAt(0));
	}
}
