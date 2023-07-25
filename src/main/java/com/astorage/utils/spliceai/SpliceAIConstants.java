package com.astorage.utils.spliceai;

public interface SpliceAIConstants {
	// General:
	String SPLICEAI_FORMAT_NAME = "SpliceAI";
	String COLUMNS_DELIMITER = "\t";
	String INFO_FIELDS_DELIMITER = "\\|";
	String LETTER_CHROMOSOMES = "XYM";
	String COMMENT_LINE_PREFIX = "##";
	String INFO_LINE_PREFIX = "##INFO";
	String INFO_LINE_FIELD_NAME_FORMAT_PREFIX = "Format: ";
	String INFO_LINE_FORMAT_SPEC_DELIMITER = "\\|";
	String COLUMN_NAMES_LINE_PREFIX = "#";
	String INFO_DS_FIELD_PREFIX = "DS_";

	// Ingestion/query request params:
	String DATA_PATH_PARAM = "dataPath";
	String CHR_PARAM = "chr";
	String POS_PARAM = "pos";
	String CHR_COLUMN_NAME = "CHROM";
	String POS_COLUMN_NAME = "POS";
	String INFO_COLUMN_NAME = "INFO";

	// Success messages:
	String INGESTION_FINISH_MSG = "All Data has been ingested.";

	// Error messages:
	String INVALID_FILE_CONTENT = "Invalid file content...";
	String INVALID_CHR_OR_POS_ERROR = "Invalid 'chr' or 'pos'...";
	String VARIANT_NOT_FOUND_ERROR = "Variant doesn't exist for given 'chr' and 'pos'...";
}
