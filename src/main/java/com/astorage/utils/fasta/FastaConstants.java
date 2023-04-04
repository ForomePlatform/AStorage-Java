package com.astorage.utils.fasta;

public interface FastaConstants {
	// General:
	String FASTA_FORMAT_NAME = "Fasta";

	// Ingestion/query request params:
	String ARRAY_NAME_PARAM = "arrayName";
	String DATA_URL_PARAM = "dataURL";
	String METADATA_URL_PARAM = "metadataURL";
	String SECTION_NAME_PARAM = "sectionName";
	String START_POS_PARAM = "startPosition";
	String END_POS_PARAM = "endPosition";

	// Other:
	String METADATA_FILENAME = "metadata";
	String COMPRESSED_DATA_FILENAME = "data.gz";
	String DATA_FILENAME = "data";

	// Error messages:
	String COLUMN_FAMILY_NULL_ERROR = "Array with the given name doesn't exist...";
}
