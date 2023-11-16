package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.gtex.GTExConstants;
import com.astorage.utils.gtex.Gene;
import com.astorage.utils.gtex.GeneToTissue;
import com.astorage.utils.gtex.Tissue;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class GTExQuery extends SingleFormatQuery implements Constants, GTExConstants {
	public GTExQuery(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void queryHandler() throws IOException {
		HttpServerRequest req = context.request();

		if (req.params().contains(DATA_TYPE_PARAM)) {
			String dataType = req.getParam(DATA_TYPE_PARAM);

			if (
				dataType.equals(GENE_COLUMN_FAMILY_NAME)
					&& req.params().contains(GENE_ID_PARAM)
					&& req.params().contains(SUB_ID_PARAM)
			) {
				String geneId = req.getParam(GENE_ID_PARAM);
				String subId = req.getParam(SUB_ID_PARAM);

				singleQueryHandlerForGene(geneId, subId, false);
			}

			if (
				dataType.equals(TISSUE_COLUMN_FAMILY_NAME)
					&& req.params().contains(TISSUE_NUMBER_PARAM)
			) {
				String tissueNo = req.getParam(TISSUE_NUMBER_PARAM);

				singleQueryHandlerForTissue(tissueNo, false);
			}

			if (
				dataType.equals(GENE_TO_TISSUE_COLUMN_FAMILY_NAME)
					&& req.params().contains(GENE_ID_PARAM)
					&& req.params().contains(SUB_ID_PARAM)
					&& req.params().contains(TISSUE_NUMBER_PARAM)
			) {
				String geneId = req.getParam(GENE_ID_PARAM);
				String subId = req.getParam(SUB_ID_PARAM);
				String tissueNo = req.getParam(TISSUE_NUMBER_PARAM);

				singleQueryHandlerForGeneToTissue(geneId, subId, tissueNo, false);
			}
		}

		Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);
	}

	protected void singleQueryHandlerForGene(String geneId, String subId, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();

		ColumnFamilyHandle geneColumnFamilyHandle = dbRep.getColumnFamilyHandle(GENE_COLUMN_FAMILY_NAME);
		if (geneColumnFamilyHandle == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, COLUMN_FAMILY_NULL_ERROR);

			return;
		}

		byte[] compressedGene = dbRep.getBytes(
			Gene.generateKey(geneId, subId),
			geneColumnFamilyHandle
		);
		if (compressedGene == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, GENE_RESULT_NOT_FOUND_ERROR);

			return;
		}

		String decompressedGene = Constants.decompressJson(compressedGene);
		JsonObject result = new JsonObject(decompressedGene);

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "application/json")
				.end(result + "\n");
		}
	}

	protected void singleQueryHandlerForTissue(String tissueNo, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();

		try {
			Integer.parseInt(tissueNo);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_TISSUE_NUMBER_ERROR);

			return;
		}

		ColumnFamilyHandle tissueColumnFamilyHandle = dbRep.getColumnFamilyHandle(TISSUE_COLUMN_FAMILY_NAME);
		if (tissueColumnFamilyHandle == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, COLUMN_FAMILY_NULL_ERROR);

			return;
		}

		byte[] compressedTissue = dbRep.getBytes(
			Tissue.generateKey(tissueNo),
			tissueColumnFamilyHandle
		);
		if (compressedTissue == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, TISSUE_RESULT_NOT_FOUND_ERROR);

			return;
		}

		String decompressedTissue = Constants.decompressJson(compressedTissue);
		JsonObject result = new JsonObject(decompressedTissue);

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "application/json")
				.end(result + "\n");
		}
	}

	protected void singleQueryHandlerForGeneToTissue(String geneId, String subId, String tissueNo, boolean isBatched) throws IOException {
		HttpServerRequest req = context.request();

		try {
			Integer.parseInt(tissueNo);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_TISSUE_NUMBER_ERROR);

			return;
		}

		ColumnFamilyHandle geneToTissueColumnFamilyHandle = dbRep.getColumnFamilyHandle(GENE_TO_TISSUE_COLUMN_FAMILY_NAME);
		if (geneToTissueColumnFamilyHandle == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, COLUMN_FAMILY_NULL_ERROR);

			return;
		}

		byte[] compressedGeneToTissue = dbRep.getBytes(
			GeneToTissue.generateKey(geneId, subId, tissueNo),
			geneToTissueColumnFamilyHandle
		);
		if (compressedGeneToTissue == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, GENE_TO_TISSUE_RESULT_NOT_FOUND_ERROR);

			return;
		}

		String decompressedGeneToTissue = Constants.decompressJson(compressedGeneToTissue);
		JsonObject result = new JsonObject(decompressedGeneToTissue);

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "application/json")
				.end(result + "\n");
		}
	}
}
