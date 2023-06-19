package com.astorage.utils;

import com.astorage.utils.dbnsfp.DbNSFPConstants;
import com.astorage.utils.fasta.FastaConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public interface Constants {
	// General:
	String USER_HOME = System.getProperty("user.home");
	String ASTORAGE_DIRECTORY_NAME = "/AStorage";
	String DATA_DIRECTORY_PATH = System.getProperty("user.home") + "/AStorage";
	String DATA_DIRECTORY_PATH_JSON_KEY = "data_directory_path";
	String[] FORMAT_NAMES = {
		FastaConstants.FASTA_FORMAT_NAME,
		DbNSFPConstants.DBNSFP_FORMAT_NAME
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
	String COMPRESSION_ERROR = "Error while compressing JSON string...";
	String DECOMPRESSION_ERROR = "Error while decompressing JSON string...";

	// Helper functions:
	static JsonArray listToJson(List<? extends JsonConvertible> list) {
		JsonArray listJson = new JsonArray();

		for (JsonConvertible item : list) {
			JsonObject itemJson = item.toJson();
			listJson.add(itemJson);
		}

		return listJson;
	}

	static byte[] compressJSON(String json) throws IOException {
		try (
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)
		) {
			gzipOutputStream.write(json.getBytes(StandardCharsets.UTF_8));
			gzipOutputStream.finish();

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new IOException(COMPRESSION_ERROR, e);
		}
	}

	static String decompressJSON(byte[] compressedData) throws IOException {
		try (
			ByteArrayInputStream inputStream = new ByteArrayInputStream(compressedData);
			GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
			ByteArrayOutputStream output = new ByteArrayOutputStream()
		) {
			byte[] buffer = new byte[1024];

			int bytesRead;
			while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
				output.write(buffer, 0, bytesRead);
			}

			return output.toString(StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new IOException(DECOMPRESSION_ERROR, e);
		}
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
}
