package com.astorage.utils;

import io.vertx.core.http.HttpServerRequest;

public interface Constants {
	String ERROR_INVALID_PARAMS = "Error: Invalid parameters";
	String ERROR_DOWNLOADING_DATA = "Error: Download failed";
	String ERROR_INITIALIZING_DIRECTORY = "Error: Couldn't initialize directories";
	String COLUMN_FAMILY_NULL = "Error: Array with the given name doesn't exist";
	String DATA_DIRECTORY_PATH = System.getProperty("user.home") + "/AStorage";
	String DATA_FILENAME = "data";
	String[] FORMAT_NAMES = {
		com.astorage.utils.fasta.Constants.FASTA_FORMAT_NAME,
		com.astorage.utils.dbnsfp.Constants.DBNSFP_FORMAT_NAME
	};

	static void errorResponse(HttpServerRequest req, int errorCode, String errorMsg) {
		req.response()
			.setStatusCode(errorCode)
			.putHeader("content-type", "text/plain")
			.end(errorMsg + "\n");
	}
}
