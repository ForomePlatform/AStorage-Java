package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.normalization.VariantNormalizer;
import com.astorage.utils.Constants;
import com.astorage.utils.gnomad.*;
import com.astorage.utils.universal_variant.UniversalVariantConstants;
import com.astorage.utils.universal_variant.UniversalVariantHelper;
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
 * For gnomAD v2.1.1!
 */
@SuppressWarnings("unused")
public class GnomADIngestor extends Ingestor implements Constants, GnomADConstants {
	public GnomADIngestor(
		RoutingContext context,
		RocksDBRepository dbRep,
		RocksDBRepository universalVariantDbRep,
		RocksDBRepository fastaDbRep
	) {
		super(context, dbRep, universalVariantDbRep, fastaDbRep);
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 2
				|| !req.params().contains(DATA_URL_PARAM)
				|| !req.params().contains(SOURCE_TYPE_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String dataURL = req.getParam(DATA_URL_PARAM);
		String sourceType = req.getParam(SOURCE_TYPE_PARAM);

		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(sourceType);
		if (columnFamilyHandle == null) {
			if (sourceType.length() == 1 && SOURCE_TYPES.contains(sourceType.toLowerCase())) {
				columnFamilyHandle = dbRep.createColumnFamily(sourceType);
			} else {
				JsonObject errorJson = new JsonObject();
				errorJson.put(ERROR, INVALID_SOURCE_TYPE_ERROR);

				Constants.errorResponse(
					req,
					HttpURLConnection.HTTP_BAD_REQUEST,
					errorJson.toString()
				);

				return;
			}
		}

		try {
			Constants.downloadUsingStream(dataURL, COMPRESSED_DATA_FILENAME);
			File file = new File(DATA_DIRECTORY_PATH, COMPRESSED_DATA_FILENAME);
			InputStream fileInputStream = new FileInputStream(file);
			InputStream gzipInputStream = new GZIPInputStream(fileInputStream);
			Reader decoder = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedReader = new BufferedReader(decoder);

			storeData(bufferedReader, columnFamilyHandle, sourceType);
		} catch (Exception e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, DOWNLOADING_DATA_ERROR);

			return;
		}

		String response = INGESTION_FINISH_MSG + "\n";

		req.response()
			.putHeader("content-type", "text/plain")
			.end(response);
	}

	private void storeData(
		BufferedReader reader,
		ColumnFamilyHandle columnFamilyHandle,
		String sourceType
	) throws Exception {
		String line;

		do {
			line = reader.readLine();
		} while (line != null && line.startsWith(COMMENT_LINE_PREFIX));

		Map<String, Integer> columns;

		if (line == null || !line.startsWith(COLUMN_NAMES_LINE_PREFIX)) {
			Constants.errorResponse(context.request(), HttpURLConnection.HTTP_BAD_REQUEST, INVALID_FILE_CONTENT);

			return;
		}

		columns = Constants.mapColumns(line.substring(1), COLUMNS_DELIMITER);

		while ((line = reader.readLine()) != null) {
			String[] values = line.split(COLUMNS_DELIMITER);
			Variant variant = new Variant(columns, values);

			String chr = values[columns.get(CHR_COLUMN_NAME)];
			String pos = values[columns.get(POS_COLUMN_NAME)];
			byte[] key = GnomADHelper.createKey(chr, pos);
			byte[] compressedVariant = Constants.compressJson(variant.toString());

			ingestQueryParams(chr, pos, variant, sourceType);

			dbRep.saveBytes(key, compressedVariant, columnFamilyHandle);
		}

		reader.close();
	}

	private void ingestQueryParams(String chr, String pos, Variant variant, String sourceType) throws Exception {
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
			pos,
			sourceType
		);

		UniversalVariantHelper.ingestUniversalVariant(
			universalVariantKey,
			variantQuery,
			GNOMAD_FORMAT_NAME,
			universalVariantDbRep
		);
	}
}
