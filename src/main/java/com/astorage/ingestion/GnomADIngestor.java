package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.normalization.VariantNormalizer;
import com.astorage.utils.Constants;
import com.astorage.utils.gnomad.GnomADConstants;
import com.astorage.utils.gnomad.GnomADHelper;
import com.astorage.utils.gnomad.Variant;
import com.astorage.utils.universal_variant.UniversalVariantConstants;
import com.astorage.utils.universal_variant.UniversalVariantHelper;
import com.astorage.utils.variant_normalizer.VariantNormalizerConstants;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * For gnomAD v4!
 */
@SuppressWarnings("unused")
public class GnomADIngestor extends Ingestor implements Constants, GnomADConstants {
	// Reference build to be used during normalization
	private String refBuild;

	// Used to keep track of the progress
	private long lineCount = 0;
	private long normalizationsCount = 0;

	public GnomADIngestor(
		RoutingContext context,
		RocksDBRepository dbRep,
		RocksDBRepository universalVariantDbRep,
		RocksDBRepository fastaDbRep,
		Vertx vertx
	) {
		super(context, dbRep, universalVariantDbRep, fastaDbRep, vertx);
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();

		if (!req.params().contains(DATA_PATH_PARAM) || !req.params().contains(SOURCE_TYPE_PARAM)) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String dataPath = req.getParam(DATA_PATH_PARAM);
		String sourceType = req.getParam(SOURCE_TYPE_PARAM);
		String normalizeParam = req.getParam(VariantNormalizerConstants.NORMALIZE_PARAM);
		boolean normalize = "true".equals(normalizeParam);

		if (normalize && !req.params().contains(VariantNormalizerConstants.REF_BUILD_PARAM)) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		this.refBuild = req.getParam(VariantNormalizerConstants.REF_BUILD_PARAM);

		File file = new File(dataPath);
		if (!file.exists()) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, FILE_NOT_FOUND_ERROR);

			return;
		}

		if (sourceType.length() != 1 || !SOURCE_TYPES.contains(sourceType.toLowerCase())) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_SOURCE_TYPE_ERROR);

			return;
		}

		try (
			InputStream fileInputStream = new FileInputStream(file);
			InputStream gzipInputStream = new GZIPInputStream(fileInputStream);
			Reader decoder = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedReader = new BufferedReader(decoder)
		) {
			boolean success = storeData(bufferedReader, sourceType, normalize);

			if (success) {
				StringBuilder successMsg = new StringBuilder();
				successMsg
					.append(lineCount)
					.append(" lines have been ingested in ")
					.append(dbRep.dbName);
				if (normalize) {
					successMsg
						.append(" out of which ")
						.append(normalizationsCount)
						.append(" have been normalized");
				}
				successMsg.append("!");

				Constants.successResponse(req, successMsg.toString());
			}
		} catch (Exception e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private boolean storeData(BufferedReader reader, String sourceType, boolean normalize) throws Exception {
		HttpServerRequest req = context.request();
		ColumnFamilyHandle columnFamilyHandle = dbRep.getOrCreateColumnFamily(sourceType);
		String line;

		do {
			line = reader.readLine();
		} while (line != null && line.startsWith(COMMENT_LINE_PREFIX));

		Map<String, Integer> columns;

		if (line == null || !line.startsWith(COLUMN_NAMES_LINE_PREFIX)) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_FILE_CONTENT);

			return false;
		}

		columns = Constants.mapColumns(line.substring(1), COLUMNS_DELIMITER);

		while ((line = reader.readLine()) != null) {
			String[] values = line.split(COLUMNS_DELIMITER);
			Variant variant = new Variant(columns, values);

			String chr = values[columns.get(CHR_COLUMN_NAME)];
			if (chr.contains(CHR_PREFIX)) {
				chr = chr.substring(CHR_PREFIX.length());
			}

			String pos = values[columns.get(POS_COLUMN_NAME)];
			byte[] key = GnomADHelper.createKey(chr, pos);
			byte[] compressedVariant = Constants.compressJson(variant.toString());

			dbRep.saveBytes(key, compressedVariant, columnFamilyHandle);

			if (normalize) {
				boolean queryParamsIngested = ingestQueryParams(chr, pos, variant, sourceType);
				if (queryParamsIngested) {
					normalizationsCount++;
				}
			}

			lineCount++;

			Constants.logProgress(dbRep, lineCount, normalize, normalizationsCount, 100000);
		}

		return true;
	}

	private boolean ingestQueryParams(String chr, String pos, Variant variant, String sourceType) throws Exception {
		if (this.refBuild.isEmpty()) {
			return false;
		}

		String ref = variant.variantColumnValues.get(REF_COLUMN_NAME);
		String alt = variant.variantColumnValues.get(ALT_COLUMN_NAME);

		JsonObject normalizedVariantJson;
		try {
			normalizedVariantJson = VariantNormalizer.normalizeVariant(
				this.refBuild,
				chr,
				pos,
				ref,
				alt,
				fastaDbRep
			);
		} catch (Exception e) {
			return false;
		}

		byte[] universalVariantKey = UniversalVariantHelper.generateKey(normalizedVariantJson);

		// Param ordering should match query specification
		String variantQuery = String.join(
			UniversalVariantConstants.QUERY_PARAMS_DELIMITER,
			chr,
			pos,
			sourceType
		);

		UniversalVariantHelper.ingestUniversalVariant(
			universalVariantKey,
			variantQuery,
			GNOMAD_FORMAT_NAME,
			universalVariantDbRep
		);

		return true;
	}
}
