package com.astorage.utils.pharmgkb;

import java.util.Arrays;
import java.util.List;

public interface PharmGKBConstants {
	String PHARMGKB_FORMAT_NAME = "PharmGKB";
	String DATA_URL_PARAM = "dataURL";
	String DATA_TYPE_PARAM = "dataType";
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
	String INVALID_DATA_TYPE_ERROR = "Invalid data type, should be either \"g\" or \"e\"...";
	String COMPRESSED_DATA_FILENAME = "data.gz";
	String COLUMNS_DELIMITER = "\t";
}
