package com.astorage.utils.dbsnp;

public interface DbSNPConstants {
	// General:
	String DBSNP_FORMAT_NAME = "DbSNP";
	String DATA_DELIMITER = "\t";
	String CHR_COLUMN_NAME = "#CHROM";
	String POS_COLUMN_NAME = "POS";
	String VARIANTS_KEY = "variants";

	// Ingestion/query request params:
	String DATA_PATH_PARAM = "dataPath";
	String CHR_PARAM = "chr";
	String POS_PARAM = "pos";
	String ALT_PARAM = "alt";

	// Error messages:
	String INVALID_DBSNP_FILE = "Invalid dbSNP file...";
	String INVALID_CHR_OR_POS_ERROR = "Invalid 'chr' or 'pos'...";
	String INVALID_ALT_ERROR = "Invalid 'alt'...";
	String VARIANT_NOT_FOUND_ERROR = "Variant doesn't exist for given 'chr' and 'pos'...";
}
