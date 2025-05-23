package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.gtf.GTFConstants;
import com.astorage.utils.gtf.Variant;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("unused")
public class GTFIngestor extends Ingestor implements Constants, GTFConstants {
	public GTFIngestor(
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
				Constants.successResponse(req, INGESTION_FINISH_MSG);
			}
		} catch (IOException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private boolean storeData(BufferedReader reader) throws IOException {
		HttpServerRequest req = context.request();
		String line;

		do {
			line = reader.readLine();
		} while (line != null && line.startsWith(COMMENT_LINE_PREFIX));

		if (line == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_FILE_CONTENT);

			return false;
		}

		long lineCount = 0;
		while ((line = reader.readLine()) != null) {
			List<String> values = Arrays.stream(line.split(COLUMNS_DELIMITER)).map(String::strip).toList();
			Variant variant = new Variant(values);

			byte[] key = variant.getKey();
			byte[] compressedVariant = Constants.compressJson(variant.toString());

			dbRep.saveBytes(key, compressedVariant);

			lineCount++;

			Constants.logProgress(dbRep, lineCount, 100000);
		}

		reader.close();

		return true;
	}
}
