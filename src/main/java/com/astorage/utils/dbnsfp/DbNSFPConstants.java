package com.astorage.utils.dbnsfp;

public interface DbNSFPConstants {
	// General:
	String DBNSFP_FORMAT_NAME = "DbNSFP";
	String DATA_DELIMITER = "\t";
	String NUCLEOTIDES = "AGTCU";
	String LETTER_CHROMOSOMES = "XYM";
	String CHR_COLUMN_NAME = "#chr";
	String POS_COLUMN_NAME = "pos(1-based)";
	String COMPACTED_DATA_KEY = "data";
	String VARIANTS_KEY = "variants";
	String TRANSCRIPTS_KEY = "transcripts";
	String FACETS_KEY = "facets";
	String NULL_SHORTHAND = ".";

	// Ingestion/query request params:
	String DATA_PATH_PARAM = "data-path";
	String CHR_PARAM = "chr";
	String POS_PARAM = "pos";
	String ALT_PARAM = "alt";

	// Error messages:
	String INVALID_CHR_OR_POS_ERROR = "Invalid 'chr' or 'pos'...";
	String INVALID_ALT_ERROR = "Invalid 'alt'...";
	String VARIANT_NOT_FOUND_ERROR = "Variant doesn't exist for given 'chr' and 'pos'...";
	String DB_DATA_NOT_CONSISTENT_ERROR = "Retrieved data from DB isn't consistent with the specification of dbNSFP...";
}
