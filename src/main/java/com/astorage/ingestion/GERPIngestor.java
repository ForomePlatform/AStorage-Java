package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.gerp.GERPConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.astorage.utils.gerp.GERPHelper.createKey;

@SuppressWarnings("unused")
public class GERPIngestor extends Ingestor implements Constants, GERPConstants {
	private String chromosome;

	public GERPIngestor(
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

		if (!this.detectChromosomeFromFilename(file.getName())) {
			return;
		}

		try (
			InputStream fileInputStream = new FileInputStream(file);
			Reader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader)
		) {
			storeData(bufferedReader);
		} catch (IOException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}

		Constants.successResponse(req, INGESTION_FINISH_MSG);
	}

	private boolean detectChromosomeFromFilename(String filename) {
		HttpServerRequest req = context.request();

		int startIdx = filename.indexOf(FILENAME_CHROMOSOME_PREFIX) + FILENAME_CHROMOSOME_PREFIX.length();
		int endIdx = filename.indexOf(FILENAME_CHROMOSOME_SUFFIX, startIdx);

		if (startIdx == FILENAME_CHROMOSOME_PREFIX.length() - 1 || endIdx == -1) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, CHROMOSOME_NOT_DETECTED_IN_FILENAME);

			return false;
		}

		this.chromosome = filename.substring(startIdx, endIdx);
		return true;
	}

	private void storeData(BufferedReader reader) throws IOException {
		String line;

		long lineCount = 0;
		while ((line = reader.readLine()) != null) {
			JsonArray values = Arrays.stream(line.split(COLUMNS_DELIMITER)).reduce(
				new JsonArray(),
				JsonArray::add,
				(JsonArray arr1, JsonArray arr2) -> null
			);

			byte[] key = createKey(this.chromosome, String.valueOf(lineCount + 1));
			byte[] compressedVariant = Constants.compressJson(values.toString());

			dbRep.saveBytes(key, compressedVariant);

			lineCount++;

			Constants.logProgress(dbRep, lineCount, 100000);
		}

		reader.close();
	}
}
