package com.astorage.utils.gtf;

public interface GTFConstants {
    // General
    String GTF_FORMAT_NAME = "GTF";
    String COLUMNS_DELIMITER = "\t";
    String ATTRIBUTES_DELIMITER = ";";
    String ATTRIBUTE_PAIR_DELIMITER = " ";
    String COMMENT_LINE_PREFIX = "#!";

    // Ingestion/query request params:
    String DATA_PATH_PARAM = "dataPath";
    String CHR_PARAM = "chr";
    String START_POS_PARAM = "startPos";
    String END_POS_PARAM = "endPos";

    // Other
    String CHROMOSOME_COLUMN_NAME = "chromosome";
    String START_POSITION_COLUMN_NAME = "start";
    String END_POSITION_COLUMN_NAME = "end";

    // Success messages:
    String INGESTION_FINISH_MSG = "All data has been ingested.";

    // Error messages:
    String INVALID_FILE_CONTENT = "Invalid file content...";
    String INVALID_CHR_OR_POS_ERROR = "Invalid 'chr', 'startPos' or 'endPos'...";
    String VARIANT_NOT_FOUND_ERROR = "Variant doesn't exist for given 'chr', 'startPos' and 'endPos'...";
}
