package com.astorage.utils.fasta;

public interface FastaConstants {
	// General:
	String FASTA_FORMAT_NAME = "Fasta";

	// Ingestion/query request params:
	String REF_BUILD_PARAM = "refBuild";
	String DATA_PATH_PARAM = "dataPath";
	String METADATA_PATH_PARAM = "metadataPath";
	String CHR_PARAM = "chr";
	String START_POS_PARAM = "startPos";
	String END_POS_PARAM = "endPos";

	// Error messages:
	String INVALID_CHR_START_POS_OR_END_POS_ERROR = "Invalid 'chr', 'start_pos' or 'end_pos'...";
	String COLUMN_FAMILY_NULL_ERROR = "Array with the given name doesn't exist...";
}
