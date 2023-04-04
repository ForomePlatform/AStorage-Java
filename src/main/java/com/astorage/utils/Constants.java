package com.astorage.utils;

import com.astorage.utils.dbnsfp.DbNSFPConstants;
import com.astorage.utils.fasta.FastaConstants;
import io.vertx.core.http.HttpServerRequest;

public interface Constants {
	// General:
	String DATA_DIRECTORY_PATH = System.getProperty("user.home") + "/AStorage";
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

	static void errorResponse(HttpServerRequest req, int errorCode, String errorMsg) {
		req.response()
			.setStatusCode(errorCode)
			.putHeader("content-type", "text/plain")
			.end(errorMsg + "\n");
	}
}
