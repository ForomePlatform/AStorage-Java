package com.astorage.utils.fasta;

public interface FastaConstants {
	// General:
	String FASTA_FORMAT_NAME = "Fasta";

	// Ingestion/query request params:
	String ARRAY_NAME_PARAM = "arrayName";
	String DATA_PATH_PARAM = "dataPath";
	String METADATA_PATH_PARAM = "metadataPath";
	String SECTION_NAME_PARAM = "sectionName";
	String START_POS_PARAM = "startPos";
	String END_POS_PARAM = "endPos";

	// Other:
	String METADATA_FILENAME = "metadata";
	String COMPRESSED_DATA_FILENAME = "data.gz";
	String DATA_FILENAME = "data";

	// Error messages:
	String COLUMN_FAMILY_NULL_ERROR = "Array with the given name doesn't exist...";
}
