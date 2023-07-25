package com.astorage.utils.clinvar;

import java.util.Arrays;
import java.util.List;

public interface ClinVarConstants {
	// General
	String CLINVAR_FORMAT_NAME = "ClinVar";

	// Ingestion/query request params:
	String DATA_PATH_PARAM = "dataPath";
	String DATA_SUMMARY_PATH_PARAM = "dataSummaryPath";
//	String DATA_TYPE_PARAM = "dataType";
	String CHR_PARAM = "chr";
	String START_POS_PARAM = "startPos";
	String END_POS_PARAM = "endPos";

	// Other:
	String LETTER_CHROMOSOMES = "XYM";
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
	String CHROMOSOME_COLUMN_NAME = "Chromosome";
	String START_POSITION_COLUMN_NAME = "Start";
	String END_POSITION_COLUMN_NAME = "Stop";
	String SUBMITTER_ID_COLUMN_NAME = "SubmitterID";
	String RCV_ACCESSION_COLUMN_NAME = "RCVaccession";

	// Error messages:
	String COLUMN_FAMILY_NULL_ERROR = "Data type with the given name doesn't exist...";
	String RESULT_NOT_FOUND_ERROR = "Result doesn't exist for given 'id'";
	String INVALID_FILE_CONTENT = "Invalid file content format...";
	String INVALID_CHR_OR_POS_ERROR = "Invalid 'chr' or 'pos'...";
}
