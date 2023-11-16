package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.clinvar.ClinVarConstants;
import com.astorage.utils.clinvar.Significance;
import com.astorage.utils.clinvar.Submitter;
import com.astorage.utils.clinvar.Variant;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class ClinVarQuery extends SingleFormatQuery implements Constants, ClinVarConstants {
	public ClinVarQuery(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void queryHandler() throws IOException {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 3
				|| !req.params().contains(CHR_PARAM)
				|| !req.params().contains(START_POS_PARAM)
				|| !req.params().contains(END_POS_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String chr = req.getParam(CHR_PARAM);
		String startPos = req.getParam(START_POS_PARAM);
		String endPos = req.getParam(END_POS_PARAM);

		singleQueryHandler(chr, startPos, endPos, false);
	}

	protected void singleQueryHandler(String chr, String startPos, String endPos, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(startPos);
			Long.parseLong(endPos);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_CHR_OR_POS_ERROR);

			return;
		}

		try {
			JsonObject result = queryData(dbRep, chr, startPos, endPos);

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

	public static JsonObject queryData(RocksDBRepository dbRep, String chr, String startPos, String endPos) throws Exception {
		ColumnFamilyHandle variantColumnFamilyHandle = dbRep.getColumnFamilyHandle(VARIANT_SUMMARY_COLUMN_FAMILY_NAME);
		ColumnFamilyHandle significanceColumnFamilyHandle = dbRep.getColumnFamilyHandle(SIGNIFICANCE_COLUMN_FAMILY_NAME);
		ColumnFamilyHandle submitterColumnFamilyHandle = dbRep.getColumnFamilyHandle(SUBMITTER_COLUMN_FAMILY_NAME);
		if (
			variantColumnFamilyHandle == null
				|| significanceColumnFamilyHandle == null
				|| submitterColumnFamilyHandle == null
		) {
			throw new Exception(COLUMN_FAMILY_NULL_ERROR);
		}

		byte[] compressedVariant = dbRep.getBytes(Variant.generateKey(chr, startPos, endPos), variantColumnFamilyHandle);
		if (compressedVariant == null) {
			throw new Exception(RESULT_NOT_FOUND_ERROR);
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);
		JsonObject result = new JsonObject(decompressedVariant);

		JsonArray significancesJson = new JsonArray();

		String[] rcvAccessions = result.getString(RCV_ACCESSION_COLUMN_NAME).split(RCV_ACCESSIONS_DELIMITER);
		for (String rcvAccession : rcvAccessions) {
			byte[] compressedSignificance = dbRep.getBytes(Significance.generateKey(rcvAccession), significanceColumnFamilyHandle);
			if (compressedSignificance != null) {
				String decompressedSignificance = Constants.decompressJson(compressedSignificance);
				JsonObject significanceJson = new JsonObject(decompressedSignificance);

				// Add submitter info to significance json item
				String submitterId = significanceJson.getString(SUBMITTER_ID_COLUMN_NAME);
				byte[] compressedSubmitter = dbRep.getBytes(Submitter.generateKey(submitterId), submitterColumnFamilyHandle);

				if (compressedSubmitter != null) {
					String decompressedSubmitter = Constants.decompressJson(compressedSubmitter);
					JsonObject submitterJson = new JsonObject(decompressedSubmitter);
					significanceJson.put(SUBMITTER_JSON_KEY, submitterJson);
				}

				significancesJson.add(significanceJson);
			}
		}

		result.put(SIGNIFICANCES_JSON_KEY, significancesJson);

		return result;
	}
}
