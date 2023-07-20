package com.astorage.utils.clinvar;

import java.util.Arrays;
import java.util.List;

public interface ClinVarConstants {
	// General
	String CLINVAR_FORMAT_NAME = "ClinVar";

	// Ingestion/query request params:
	String DATA_PATH_PARAM = "dataPath";
	String DATA_SUMMARY_PATH_PARAM = "dataSummaryPath";
	String DATA_TYPE_PARAM = "dataType";
	String ID_PARAM = "id";

	// Other:
	String SIGNIFICANCE_COLUMN_FAMILY_NAME = "significance";
	String SUBMITTER_COLUMN_FAMILY_NAME = "submitter";
	String VARIANT_SUMMARY_COLUMN_FAMILY_NAME = "variant";
	List<String> DATA_TYPES = Arrays.asList(
		SIGNIFICANCE_COLUMN_FAMILY_NAME,
		SUBMITTER_COLUMN_FAMILY_NAME,
		VARIANT_SUMMARY_COLUMN_FAMILY_NAME
	);
	String COLUMNS_DELIMITER = "\t";
	String COLUMN_NAMES_LINE_PREFIX = "#";
	String ALLELE_ID_COLUMN_NAME = "AlleleID";
	String SUBMITTER_ID_COLUMN_NAME = "SubmitterID";

	// Error messages:
	String INVALID_DATA_TYPE_ERROR = "Invalid data type, should be either \"g\" or \"e\"...";
	String COLUMN_FAMILY_NULL_ERROR = "Data type with the given name doesn't exist...";
	String RESULT_NOT_FOUND_ERROR = "Result doesn't exist for given 'id'";
	String INVALID_FILE_CONTENT = "Invalid file content format...";
}
