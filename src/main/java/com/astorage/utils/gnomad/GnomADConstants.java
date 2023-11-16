package com.astorage.utils.gnomad;

public interface GnomADConstants {
	// General:
	String GNOMAD_FORMAT_NAME = "GnomAD";
	String COLUMNS_DELIMITER = "\t";
	String INFO_FIELDS_DELIMITER = ";";
	String INFO_FIELD_KEY_VALUE_DELIMITER = "=";
	String SOURCE_TYPES = "ge"; // g - genome, e - exome
	String COMMENT_LINE_PREFIX = "##";
	String COLUMN_NAMES_LINE_PREFIX = "#";

	// Ingestion/query request params:
	String DATA_URL_PARAM = "dataURL";
	String SOURCE_TYPE_PARAM = "sourceType";
	String SOURCE_TYPE_FIELD_NAME = "SOURCE";
	String CHR_PARAM = "chr";
	String POS_PARAM = "pos";
	String CHR_COLUMN_NAME = "CHROM";
	String POS_COLUMN_NAME = "POS";
	String REF_COLUMN_NAME = "REF";
	String ALT_COLUMN_NAME = "ALT";
	String INFO_COLUMN_NAME = "INFO";

	// Other:
	String COMPRESSED_DATA_FILENAME = "data.gz";

	// Success messages:
	String INGESTION_FINISH_MSG = "All data has been ingested.";

	// Error messages:
	String INVALID_FILE_CONTENT = "Invalid gnomAD file...";
	String INVALID_CHR_OR_POS_ERROR = "Invalid 'chr' or 'pos'...";
	String INVALID_SOURCE_TYPE_ERROR = "Invalid source type, should be either \"g\" or \"e\"...";
	String VARIANT_NOT_FOUND_ERROR = "Variant doesn't exist for given 'chr' and 'pos'...";
	String COLUMN_FAMILY_NULL_ERROR = "Array with the given name doesn't exist...";
}
