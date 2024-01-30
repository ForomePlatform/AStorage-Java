package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.dbsnp.DbSNPConstants;
import com.astorage.utils.dbsnp.DbSNPHelper;
import com.astorage.utils.dbsnp.Variant;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("unused")
public class DbSNPIngestor extends Ingestor implements Constants, DbSNPConstants {
	public DbSNPIngestor(
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
			String line;
			Map<String, Integer> columns = null;

			while ((line = bufferedReader.readLine()) != null) {
				if (line.startsWith("##")) {
					continue;
				}

				if (!line.startsWith(CHR_COLUMN_NAME)) {
					Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_DBSNP_FILE);

					return;
				} else {
					columns = Constants.mapColumns(line, DATA_DELIMITER);

					break;
				}
			}

			long lineCount = 0;
			byte[] lastKey = null;
			List<Variant> lastVariants = new ArrayList<>();

			while ((line = bufferedReader.readLine()) != null) {
				lastKey = processLine(line, columns, lastKey, lastVariants);
				lineCount++;

				Constants.logProgress(dbRep, lineCount, 100000);
			}

			if (!lastVariants.isEmpty()) {
				saveVariantsInDb(lastKey, lastVariants);
			}

			Constants.successResponse(req, lineCount + " lines have been ingested in " + dbRep.dbName + "!");
		} catch (Exception e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private byte[] processLine(String line, Map<String, Integer> columns, byte[] lastKey, List<Variant> lastVariants) throws Exception {
		String[] row = line.split(DATA_DELIMITER);

		Variant newVariant = new Variant(columns, row);
		byte[] key = DbSNPHelper.createKey(columns, row);

		if (Arrays.equals(key, lastKey)) {
			lastVariants.add(newVariant);
		} else {
			if (!lastVariants.isEmpty()) {
				saveVariantsInDb(lastKey, lastVariants);
			}

			lastVariants.clear();
			lastVariants.add(newVariant);
		}

		return key;
	}

	private void saveVariantsInDb(byte[] key, List<Variant> variants) throws IOException {
		JsonArray variantsJson = Constants.listToJson(variants);
		byte[] compressedVariantsJson = Constants.compressJson(variantsJson.toString());

		dbRep.saveBytes(key, compressedVariantsJson);
	}
}
