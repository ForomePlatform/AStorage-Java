package com.astorage.utils;

import com.astorage.utils.dbnsfp.DbNSFPConstants;
import com.astorage.utils.fasta.FastaConstants;
import com.astorage.utils.gnomad.GnomADConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public interface Constants {
	// General:
	String USER_HOME = System.getProperty("user.home");
	String ASTORAGE_DIRECTORY_NAME = "/AStorage";
	String DATA_DIRECTORY_PATH = System.getProperty("user.home") + "/AStorage";
	String DATA_DIRECTORY_PATH_JSON_KEY = "data_directory_path";
	String[] FORMAT_NAMES = {
		FastaConstants.FASTA_FORMAT_NAME,
		DbNSFPConstants.DBNSFP_FORMAT_NAME,
		GnomADConstants.GNOMAD_FORMAT_NAME
	};

	// Error messages:
	String ROCKS_DB_INIT_ERROR = "RocksDB couldn't initialize...";
	String INVALID_PARAMS_ERROR = "Invalid parameters...";
	String DOWNLOADING_DATA_ERROR = "Download failed...";
	String INITIALIZING_DIRECTORY_ERROR = "Couldn't initialize directories...";
	String FILE_NOT_FOUND_ERROR = "File does not exist on given path...";
	String CONFIG_JSON_DOESNT_EXIST_ERROR = "Given config file doesn't exist.";
	String CONFIG_JSON_NOT_READABLE_ERROR = "Couldn't read the given config file...";
	String CONFIG_JSON_DECODE_ERROR = "Given config file isn't a valid JSON...";
	String JSON_DECODE_ERROR = "Given file isn't a valid JSON...";

	// Helper functions:
	static JsonArray listToJson(List<? extends JsonConvertible> list) {
		JsonArray listJson = new JsonArray();

		for (JsonConvertible item : list) {
			JsonObject itemJson = item.toJson();
			listJson.add(itemJson);
		}

		return listJson;
	}

	static void errorResponse(HttpServerRequest req, int errorCode, String errorMsg) {
		if (req.response().ended()) {
			return;
		}

		if (req.response().headWritten()) {
			if (req.response().isChunked()) {
				req.response().write(errorMsg + "\n");
			} else {
				req.response().end(errorMsg + "\n");
			}

			return;
		}

		req.response()
			.setStatusCode(errorCode)
			.putHeader("content-type", "text/plain")
			.end(errorMsg + "\n");
	}

	static void downloadUsingStream(String urlStr, String filename) throws IOException {
		File file = new File(DATA_DIRECTORY_PATH, filename);
		URL url = new URL(urlStr);
		BufferedInputStream bufferedInputStream = new BufferedInputStream(url.openStream());
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		byte[] buffer = new byte[1024];
		int count;

		while ((count = bufferedInputStream.read(buffer, 0, 1024)) != -1) {
			fileOutputStream.write(buffer, 0, count);
		}

		bufferedInputStream.close();
		fileOutputStream.close();
	}

	static void decompressGzip(String compressedFilename, String decompressedFilename) throws IOException {
		File sourceFile = new File(DATA_DIRECTORY_PATH, compressedFilename);
		File targetFile = new File(DATA_DIRECTORY_PATH, decompressedFilename);

		try (
			GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(sourceFile));
			FileOutputStream fileOutputStream = new FileOutputStream(targetFile)
		) {
			byte[] buffer = new byte[1024];
			int len;
			while ((len = gzipInputStream.read(buffer)) > 0) {
				fileOutputStream.write(buffer, 0, len);
			}
		}
	}

	static Map<String, Integer> mapColumns(String line, String dataDelimiter) {
		String[] columns = line.split(dataDelimiter);
		Map<String, Integer> mappedColumns = new HashMap<>();

		for (int i = 0; i < columns.length; i++) {
			mappedColumns.put(columns[i], i);
		}

		return mappedColumns;
	}
}
