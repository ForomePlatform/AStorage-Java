package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.normalization.VariantNormalizer;
import com.astorage.utils.Constants;
import com.astorage.utils.spliceai.SpliceAIConstants;
import com.astorage.utils.spliceai.SpliceAIHelper;
import com.astorage.utils.spliceai.Variant;
import com.astorage.utils.universal_variant.UniversalVariantConstants;
import com.astorage.utils.universal_variant.UniversalVariantHelper;
import com.astorage.utils.variant_normalizer.VariantNormalizerConstants;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * For spliceAI v1.3!
 */
@SuppressWarnings("unused")
public class SpliceAIIngestor extends Ingestor implements Constants, SpliceAIConstants {
	private final Map<String, Integer> infoFieldNamesToIndices = new HashMap<>();

	// Reference build to be used during normalization
	private String refBuild;

	// Used to keep track of the progress
	private long lineCount = 0;
	private long normalizationsCount = 0;

	public SpliceAIIngestor(
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

		if (!req.params().contains(DATA_PATH_PARAM)) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String dataPath = req.getParam(DATA_PATH_PARAM);
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

		try (
			InputStream fileInputStream = new FileInputStream(file);
			InputStream gzipInputStream = new GZIPInputStream(fileInputStream);
			Reader decoder = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedReader = new BufferedReader(decoder)
		) {
			boolean success = storeData(bufferedReader, normalize);

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

	private boolean storeData(BufferedReader reader, boolean normalize) throws Exception {
		HttpServerRequest req = context.request();
		String line;

		while ((line = reader.readLine()) != null && line.startsWith(COMMENT_LINE_PREFIX)) {
			if (line.startsWith(INFO_LINE_PREFIX)) {
				storeFormatInfo(line);
			}
		}

		if (line == null || !line.startsWith(COLUMN_NAMES_LINE_PREFIX)) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_FILE_CONTENT);

			return false;
		}

		Map<String, Integer> columns = Constants.mapColumns(line.substring(1), COLUMNS_DELIMITER);

		byte[] lastKey = new byte[0];
		List<Variant> lastVariants = new ArrayList<>();
		while ((line = reader.readLine()) != null) {
			lastKey = processLine(line, columns, lastKey, lastVariants, normalize);
			lineCount++;

			Constants.logProgress(dbRep, lineCount, normalize, normalizationsCount, 100000);
		}

		if (!lastVariants.isEmpty()) {
			saveVariantsInDb(lastKey, lastVariants);
		}

		return true;
	}

	private byte[] processLine(
		String line,
		Map<String, Integer> columns,
		byte[] lastKey,
		List<Variant> lastVariants,
		boolean normalize
	) throws Exception {
		String[] values = line.split(COLUMNS_DELIMITER);
		Variant variant = new Variant(columns, values, infoFieldNamesToIndices);

		String chr = values[columns.get(CHR_COLUMN_NAME)];
		String pos = values[columns.get(POS_COLUMN_NAME)];
		byte[] key = SpliceAIHelper.createKey(chr, pos);

		if (!Arrays.equals(lastKey, key) && !lastVariants.isEmpty()) {
			saveVariantsInDb(lastKey, lastVariants);

			lastVariants.clear();
		}

		lastVariants.add(variant);

		if (normalize) {
			boolean queryParamsIngested = ingestQueryParams(chr, pos, variant);
			if (queryParamsIngested) {
				normalizationsCount++;
			}
		}

		return key;
	}

	private void saveVariantsInDb(byte[] key, List<Variant> variants) throws IOException {
		JsonArray variantsJson = new JsonArray();

		byte[] compressedVariants = dbRep.getBytes(key);
		if (compressedVariants != null) {
			String decompressedVariants = Constants.decompressJson(compressedVariants);
			variantsJson = new JsonArray(decompressedVariants);
		}

		for (Variant variant : variants) {
			variantsJson.add(variant.toJson());
		}

		byte[] updatedCompressedVariants = Constants.compressJson(variantsJson.toString());

		dbRep.saveBytes(key, updatedCompressedVariants);
	}

	private void storeFormatInfo(String infoLine) {
		int formatStartIdx = infoLine.indexOf(INFO_LINE_FIELD_NAME_FORMAT_PREFIX) + INFO_LINE_FIELD_NAME_FORMAT_PREFIX.length();
		int formatEndIdx = infoLine.length() - 2;
		infoLine = infoLine.substring(formatStartIdx, formatEndIdx);
		String[] infoFieldNames = infoLine.split(INFO_LINE_FORMAT_SPEC_DELIMITER);

		for (int i = 0; i < infoFieldNames.length; i++) {
			infoFieldNamesToIndices.put(infoFieldNames[i], i);
		}
	}

	private boolean ingestQueryParams(String chr, String pos, Variant variant) throws Exception {
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
			alt
		);

		UniversalVariantHelper.ingestUniversalVariant(
			universalVariantKey,
			variantQuery,
			SPLICEAI_FORMAT_NAME,
			universalVariantDbRep
		);

		return true;
	}
}
