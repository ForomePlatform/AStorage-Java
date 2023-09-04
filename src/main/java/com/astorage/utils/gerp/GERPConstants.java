package com.astorage.utils.gerp;

public interface GERPConstants {
	// General
	String GERP_FORMAT_NAME = "GERP";
	String COLUMNS_DELIMITER = "\t";
	String FILENAME_CHROMOSOME_PREFIX = "chr";
	String FILENAME_CHROMOSOME_SUFFIX = ".";

	// Ingestion/query request params:
	String DATA_PATH_PARAM = "dataPath";
	String CHR_PARAM = "chr";
	String POS_PARAM = "pos";

	// Success messages:
	String INGESTION_FINISH_MSG = "All Data has been ingested.";

	// Error messages:
	String CHROMOSOME_NOT_DETECTED_IN_FILENAME = "Chromosome couldn't be detected in the filename...";
	String INVALID_CHR_OR_POS_ERROR = "Invalid 'chr' or 'pos'...";
	String VARIANT_NOT_FOUND_ERROR = "Variant doesn't exist for given 'chr' and 'pos'...";
}
