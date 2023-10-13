package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.normalization.VariantNormalizer;
import com.astorage.utils.Constants;
import com.astorage.utils.spliceai.SpliceAIConstants;
import com.astorage.utils.spliceai.SpliceAIHelper;
import com.astorage.utils.spliceai.Variant;
import com.astorage.utils.universal_variant.UniversalVariantHelper;
import com.astorage.utils.universal_variant.UniversalVariantConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * For spliceAI v1.3!
 */
@SuppressWarnings("unused")
public class SpliceAIIngestor extends Ingestor implements Constants, SpliceAIConstants {
	private final Map<String, Integer> infoFieldNamesToIndices = new HashMap<>();

	public SpliceAIIngestor(
		RoutingContext context,
		RocksDBRepository dbRep,
		RocksDBRepository universalVariantDbRep,
		RocksDBRepository fastaDbRep
	) {
		super(context, dbRep, universalVariantDbRep, fastaDbRep);
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();

		if (req.params().size() != 1 || !req.params().contains(DATA_PATH_PARAM)) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String dataPath = req.getParam(DATA_PATH_PARAM);
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
			boolean success = storeData(bufferedReader);

			if (success) {
				String response = INGESTION_FINISH_MSG + "\n";

				req.response()
					.putHeader("content-type", "text/plain")
					.end(response);
			}
		} catch (Exception e) {
			Constants.errorResponse(context.request(), HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private boolean storeData(BufferedReader reader) throws Exception {
		String line;

		while ((line = reader.readLine()) != null && line.startsWith(COMMENT_LINE_PREFIX)) {
			if (line.startsWith(INFO_LINE_PREFIX)) {
				storeFormatInfo(line);
			}
		}

		if (line == null || !line.startsWith(COLUMN_NAMES_LINE_PREFIX)) {
			Constants.errorResponse(context.request(), HttpURLConnection.HTTP_BAD_REQUEST, INVALID_FILE_CONTENT);

			return false;
		}

		Map<String, Integer> columns = Constants.mapColumns(line.substring(1), COLUMNS_DELIMITER);

		while ((line = reader.readLine()) != null) {
			String[] values = line.split(COLUMNS_DELIMITER);
			Variant variant = new Variant(columns, values, infoFieldNamesToIndices);

			String chr = values[columns.get(CHR_COLUMN_NAME)];
			String pos = values[columns.get(POS_COLUMN_NAME)];
			byte[] key = SpliceAIHelper.createKey(chr, pos);
			byte[] compressedVariant = Constants.compressJson(variant.toString());

			ingestQueryParams(chr, pos, variant);

			dbRep.saveBytes(key, compressedVariant);
		}

		reader.close();

		return true;
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

	private void ingestQueryParams(String chr, String pos, Variant variant) throws Exception {
		String ref = variant.variantColumnValues.get(REF_COLUMN_NAME);
		String alt = variant.variantColumnValues.get(ALT_COLUMN_NAME);

		JsonObject normalizedVariantJson = VariantNormalizer.normalizeVariant(
			"hg38",
			chr,
			pos,
			ref,
			alt,
			fastaDbRep
		);

		byte[] universalVariantKey = UniversalVariantHelper.generateKey(normalizedVariantJson);

		// Param ordering should match query specification
		String variantQuery = String.join(
			UniversalVariantConstants.QUERY_PARAMS_DELIMITER,
			chr,
			pos
		);

		UniversalVariantHelper.ingestUniversalVariant(
			universalVariantKey,
			variantQuery,
			SPLICEAI_FORMAT_NAME,
			universalVariantDbRep
		);
	}
}
