package com.astorage.main;

public interface Constants {
	String ERROR_INVALID_PARAMS = "Error: Invalid parameters";
	String ERROR_DOWNLOADING_DATA = "Error: Download failed";
	String ERROR_INITIALIZING_DIRECTORY = "Error: Couldn't initialize directories";
	String COLUMN_FAMILY_NULL = "Error: Array with the given name doesn't exist";
	String DATA_DIRECTORY_PATH = System.getProperty("user.home") + "/AStorage";
	String METADATA_FILENAME = "metadata";
	String COMPRESSED_DATA_FILENAME = "data.gz";
	String DATA_FILENAME = "data";
}
