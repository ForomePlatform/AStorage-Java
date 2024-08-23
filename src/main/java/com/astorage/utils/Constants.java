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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public interface Constants {
	// General:
	int DEFAULT_HTTP_SERVER_PORT = 8080;
	String USER_HOME = System.getProperty("user.home");
	String ASTORAGE_DIRECTORY_NAME = "/AStorage";
	String HTTP_SERVER_START = "HTTP server started on port: %d!";
	String HTTP_SERVER_STOP = "HTTP server stopped.";

	// Config related:
	String DATA_DIRECTORY_PATH_CONFIG_KEY = "dataDirectoryPath";
	String HTTP_SERVER_PORT_CONFIG_KEY = "serverPort";

	// Executors related:
	int INGESTION_EXECUTOR_POOL_SIZE_LIMIT = 1;
	int QUERY_EXECUTOR_POOL_SIZE_LIMIT = 4;
	int EXECUTOR_TIME_LIMIT_DAYS = 7;

	static String getIngestionExecutorName(String name) {
		return name.toLowerCase() + "-ingestion-executor";
	}

	static String getQueryExecutorName(String name) {
		return name.toLowerCase() + "-query-executor";
	}

	static String getBatchQueryExecutorName(String name) {
		return name.toLowerCase() + "-batch-query-executor";
	}

	// URL paths:
	String INGESTION_URL_PATH = "/ingestion/";
	String QUERY_URL_PATH = "/query/";
	String BATCH_QUERY_URL_PATH = "/batch-query/";
	String NORMALIZATION_URL_PATH = "/normalization";
	String BATCH_NORMALIZATION_URL_PATH = "/batch-normalization";
	String STOP_URL_PATH = "/stop";

	// Format related:
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
			System.out.print(timeStamp() + dbRep.dbName + ": " + lineCount + " lines have been processed");

			if (normalize) {
				System.out.print(" out of which " + normalizationsCount + " normalized");
			}

			System.out.println("...");
		}
	}

	static void logProgress(RocksDBRepository dbRep, long lineCount, int interval) {
		logProgress(dbRep, lineCount, false, 0, interval);
	}

	static Map<String, Integer> mapColumns(String line, String dataDelimiter) {
		String[] columns = line.split(dataDelimiter);
		Map<String, Integer> mappedColumns = new HashMap<>();

		for (int i = 0; i < columns.length; i++) {
			mappedColumns.put(columns[i], i);
		}

		return mappedColumns;
	}

	static String timeStamp() {
		return new Timestamp(new Date().getTime()) + " > ";
	}
}
