package com.astorage.utils.pharmgkb;

import java.util.Arrays;
import java.util.List;

public interface PharmGKBConstants {
	// General
	String PHARMGKB_FORMAT_NAME = "PharmGKB";

	// Ingestion/query request params:
	String DATA_PATH_PARAM = "dataPath";
	String DATA_TYPE_PARAM = "dataType";
	String ID_PARAM = "id";

	// Other:
	String DATA_TYPE_FIELD_NAME = "dataType";
	List<String> DATA_TYPES = Arrays.asList(
		"CA",
		"CAmeta",
		"CAmeta2CA",
		"SPA",
		"VDA",
		"VDA2SPA",
		"VFA",
		"VFA2SPA",
		"VPA",
		"VPA2SPA"
	);
	String COLUMNS_DELIMITER = "\t";

	// Success messages:
	String INGESTION_FINISH_MSG = "All data has been ingested.";

	// Error messages:
	String INVALID_DATA_TYPE_ERROR = "Invalid data type, should be either \"g\" or \"e\"...";
	String COLUMN_FAMILY_NULL_ERROR = "Array with the given name doesn't exist...";
	String VARIANT_NOT_FOUND_ERROR = "Variant doesn't exist for given 'id'";
}
