package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.clinvar.ClinVarConstants;
import com.astorage.utils.clinvar.Significance;
import com.astorage.utils.clinvar.Submitter;
import com.astorage.utils.clinvar.Variant;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class ClinVarQuery implements Query, Constants, ClinVarConstants {
	protected final RoutingContext context;
	protected final RocksDBRepository dbRep;

	public ClinVarQuery(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
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
		JsonObject errorJson = new JsonObject();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(startPos);
			Long.parseLong(endPos);
		} catch (NumberFormatException e) {
			errorJson.put("error", INVALID_CHR_OR_POS_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		ColumnFamilyHandle variantColumnFamilyHandle = dbRep.getColumnFamilyHandle(VARIANT_SUMMARY_COLUMN_FAMILY_NAME);
		ColumnFamilyHandle significanceColumnFamilyHandle = dbRep.getColumnFamilyHandle(SIGNIFICANCE_COLUMN_FAMILY_NAME);
		ColumnFamilyHandle submitterColumnFamilyHandle = dbRep.getColumnFamilyHandle(SUBMITTER_COLUMN_FAMILY_NAME);
		if (
			variantColumnFamilyHandle == null
			|| significanceColumnFamilyHandle == null
			|| submitterColumnFamilyHandle == null
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, COLUMN_FAMILY_NULL_ERROR);
			return;
		}

		byte[] compressedVariant = dbRep.getBytes(Variant.generateKey(chr, startPos, endPos), variantColumnFamilyHandle);
		if (compressedVariant == null) {
			errorJson.put("error", RESULT_NOT_FOUND_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		String decompressedVariant = Constants.decompressJson(compressedVariant);
		JsonObject result = new JsonObject(decompressedVariant);

		String rcvAccession = result.getString(RCV_ACCESSION_COLUMN_NAME);
		byte[] compressedSignificance = dbRep.getBytes(Significance.generateKey(rcvAccession), significanceColumnFamilyHandle);
		if (compressedSignificance != null) {
			String decompressedSignificance = Constants.decompressJson(compressedSignificance);
			JsonObject significanceJson = new JsonObject(decompressedSignificance);
			result.put(SIGNIFICANCE_COLUMN_FAMILY_NAME, significanceJson);

			String submitterId = significanceJson.getString(SUBMITTER_ID_COLUMN_NAME);
			byte[] compressedSubmitter = dbRep.getBytes(Submitter.generateKey(submitterId), submitterColumnFamilyHandle);

			if (compressedSubmitter != null) {
				String decompressedSubmitter = Constants.decompressJson(compressedSubmitter);
				JsonObject submitterJson = new JsonObject(decompressedSubmitter);
				result.put(SUBMITTER_COLUMN_FAMILY_NAME, submitterJson);
			}
		}

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "text/json")
				.end(result + "\n");
		}
	}
}
