package com.astorage.utils;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.clinvar.ClinVarConstants;
import com.astorage.utils.dbnsfp.DbNSFPConstants;
import com.astorage.utils.dbsnp.DbSNPConstants;
import com.astorage.utils.fasta.FastaConstants;
import com.astorage.utils.gerp.GERPConstants;
import com.astorage.utils.gnomad.GnomADConstants;
import com.astorage.utils.gtex.GTExConstants;
import com.astorage.utils.gtf.GTFConstants;
import com.astorage.utils.pharmgkb.PharmGKBConstants;
import com.astorage.utils.spliceai.SpliceAIConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public interface Constants {
	// General:
	int HTTP_SERVER_PORT = 8080;
	int INGESTION_EXECUTOR_POOL_SIZE_LIMIT = 1;
	int QUERY_EXECUTOR_POOL_SIZE_LIMIT = 4;
	int EXECUTOR_TIME_LIMIT_DAYS = 7;
	String INGESTION_EXECUTOR_SUFFIX = "-ingestion-executor";
	String QUERY_EXECUTOR_SUFFIX = "-query-executor";
	String USER_HOME = System.getProperty("user.home");
	String ASTORAGE_DIRECTORY_NAME = "/AStorage";
	String DATA_DIRECTORY_PATH = System.getProperty("user.home") + "/AStorage";
	String DATA_DIRECTORY_PATH_JSON_KEY = "dataDirectoryPath";

	String[] FORMAT_NAMES = {
		FastaConstants.FASTA_FORMAT_NAME,
		DbNSFPConstants.DBNSFP_FORMAT_NAME,
		GnomADConstants.GNOMAD_FORMAT_NAME,
		SpliceAIConstants.SPLICEAI_FORMAT_NAME,
		PharmGKBConstants.PHARMGKB_FORMAT_NAME,
		ClinVarConstants.CLINVAR_FORMAT_NAME,
		GTExConstants.GTEX_FORMAT_NAME,
		GTFConstants.GTF_FORMAT_NAME,
		GERPConstants.GERP_FORMAT_NAME,
		DbSNPConstants.DBSNP_FORMAT_NAME
	};
	String[] UNIVERSAL_QUERY_FORMAT_NAMES = {
		DbNSFPConstants.DBNSFP_FORMAT_NAME,
		GnomADConstants.GNOMAD_FORMAT_NAME,
		SpliceAIConstants.SPLICEAI_FORMAT_NAME,
		ClinVarConstants.CLINVAR_FORMAT_NAME,
		GERPConstants.GERP_FORMAT_NAME,
		DbSNPConstants.DBSNP_FORMAT_NAME
	};
	String HTTP_SERVER_START = "HTTP server started on port: " + HTTP_SERVER_PORT + "!";
	String HTTP_SERVER_STOP = "HTTP server stopped.";

	// Variant related:
	String NUCLEOTIDES = "AGTCU";
	String LETTER_CHROMOSOMES = "XYM";
	String MITOCHONDRIAL_CHR = "M";
	String MITOCHONDRIAL_CHR_ALT = "MT";

	// Success messages:
	String SUCCESS = "success";

	// Error messages:
	String ERROR = "error";
	String ROCKS_DB_INIT_ERROR = "RocksDB couldn't initialize...";
	String INVALID_PARAMS_ERROR = "Invalid parameters...";
	String HTTP_SERVER_FAIL = "Server failed to start...";
	String DOWNLOADING_DATA_ERROR = "Download failed...";
	String INITIALIZING_DIRECTORY_ERROR = "Couldn't initialize directories...";
	String FILE_NOT_FOUND_ERROR = "File does not exist on given path...";
	String CONFIG_JSON_DOESNT_EXIST_ERROR = "Given config file doesn't exist.";
	String CONFIG_JSON_NOT_READABLE_ERROR = "Couldn't read the given config file...";
	String CONFIG_JSON_DECODE_ERROR = "Given config file isn't a valid JSON...";
	String JSON_DECODE_ERROR = "Given file isn't a valid JSON...";
	String COMPRESSION_ERROR = "Error while compressing JSON string...";
	String DECOMPRESSION_ERROR = "Error while decompressing JSON string...";
	String COLUMN_DOESNT_EXIST = "Column does not exist: ";

	// Helper functions:
	static JsonArray listToJson(List<? extends JsonConvertible> list) {
		JsonArray listJson = new JsonArray();

		for (JsonConvertible item : list) {
			JsonObject itemJson = item.toJson();
			listJson.add(itemJson);
		}

		return listJson;
	}

	static byte[] compressJson(String json) throws IOException {
		try (
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)
		) {
			gzipOutputStream.write(json.getBytes());
			gzipOutputStream.finish();

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new IOException(COMPRESSION_ERROR, e);
		}
	}

	static String decompressJson(byte[] compressedData) throws IOException {
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

	static void successResponse(HttpServerRequest req, String successMsg) {
		JsonObject successJson = new JsonObject();
		successJson.put(SUCCESS, successMsg);

		if (req.response().ended()) {
			return;
		}

		if (req.response().headWritten()) {
			if (req.response().isChunked()) {
				req.response().write(successJson + "\n");
			} else {
				req.response().end(successJson + "\n");
			}

			return;
		}

		req.response()
			.putHeader("content-type", "application/json")
			.end(successJson + "\n");
	}

	static void errorResponse(HttpServerRequest req, int errorCode, String errorMsg) {
		JsonObject errorJson = new JsonObject();
		errorJson.put(ERROR, errorMsg);

		if (req.response().ended()) {
			return;
		}

		if (req.response().headWritten()) {
			if (req.response().isChunked()) {
				req.response().write(errorJson + "\n");
			} else {
				req.response().end(errorJson + "\n");
			}

			return;
		}

		req.response()
			.setStatusCode(errorCode)
			.putHeader("content-type", "application/json")
			.end(errorJson + "\n");
	}

	static void logProgress(
		RocksDBRepository dbRep,
		long lineCount,
		boolean normalize,
		long normalizationsCount,
		int interval
	) {
		if (lineCount % interval == 0) {
			System.out.print(dbRep.dbName + ": " + lineCount + " lines have been processed");

			if (normalize) {
				System.out.print(" out of which " + normalizationsCount + " normalized");
			}

			System.out.println("...");
		}
	}

	static void logProgress(RocksDBRepository dbRep, long lineCount, int interval) {
		logProgress(dbRep, lineCount, false, 0, interval);
	}

	static void downloadUsingStream(String urlStr, String filename) throws IOException, URISyntaxException {
		File file = new File(DATA_DIRECTORY_PATH, filename);
		URI uri = new URI(urlStr);
		URL url = uri.toURL();
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

	static Map<String, Integer> mapColumns(String line, String dataDelimiter) {
		String[] columns = line.split(dataDelimiter);
		Map<String, Integer> mappedColumns = new HashMap<>();

		for (int i = 0; i < columns.length; i++) {
			mappedColumns.put(columns[i], i);
		}

		return mappedColumns;
	}
}
