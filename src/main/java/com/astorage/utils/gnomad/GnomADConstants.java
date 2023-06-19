package com.astorage.utils.gnomad;

public interface GnomADConstants {
	// General:
	String GNOMAD_FORMAT_NAME = "GnomAD";
	String COLUMNS_DELIMITER = "\t";
	String INFO_FIELDS_DELIMITER = ";";
	String INFO_FIELD_KEY_VALUE_DELIMITER = "=";
	String LETTER_CHROMOSOMES = "XYM";
	String SOURCE_TYPES = "ge";
	String COMMENT_LINE_PREFIX = "##";
	String COLUMN_NAMES_LINE_PREFIX = "#";

	// Ingestion/query request params:
	String DATA_URL_PARAM = "dataURL";
	String SOURCE_TYPE_PARAM = "sourceType";
	String CHR_PARAM = "chr";
	String POS_PARAM = "pos";
	String CHR_COLUMN_NAME = "CHROM";
	String POS_COLUMN_NAME = "POS";
	String INFO_COLUMN_NAME = "INFO";

	// Other:
	String COMPRESSED_DATA_FILENAME = "data.gz";

	// Error messages:
	String INVALID_FILE_CONTENT = "Invalid gnomAD file...";
	String INVALID_CHR_OR_POS_ERROR = "Invalid 'chr' or 'pos'...";
	String INVALID_SOURCE_TYPE_ERROR = "Invalid source type, should be either \"g\" or \"e\"...";
	String VARIANT_NOT_FOUND_ERROR = "Variant doesn't exist for given 'chr' and 'pos'...";
	String COLUMN_FAMILY_NULL_ERROR = "Array with the given name doesn't exist...";
}
