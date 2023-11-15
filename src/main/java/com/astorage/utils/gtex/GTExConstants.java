package com.astorage.utils.gtex;

public interface GTExConstants {
	// General
	String GTEX_FORMAT_NAME = "GTEx";

	// Ingestion/query request params:
	String DATA_PATH_PARAM = "dataPath";
	String DATA_TYPE_PARAM = "dataType";
	String GENE_ID_PARAM = "geneId";
	String SUB_ID_PARAM = "subId";
	String TISSUE_NUMBER_PARAM = "tissueNo";

	// Other:
	String GENE_COLUMN_FAMILY_NAME = "gene";
	String TISSUE_COLUMN_FAMILY_NAME = "tissue";
	String GENE_TO_TISSUE_COLUMN_FAMILY_NAME = "geneToTissue";
	String COLUMNS_DELIMITER = "\t";
	String GENE_ID_KEY = "GeneId";
	String SUB_ID_KEY = "SubId";
	String SYMBOL_KEY = "Symbol";
	String TISSUE_NUMBER_KEY = "TissueNo";

	// Success messages:
	String INGESTION_FINISH_MSG = "All data has been ingested.";

	// Error messages:
	String COLUMN_FAMILY_NULL_ERROR = "Data type with the given name doesn't exist...";
	String GENE_RESULT_NOT_FOUND_ERROR = "Gene record doesn't exist for given parameters...";
	String TISSUE_RESULT_NOT_FOUND_ERROR = "Tissue record doesn't exist for given parameters...";
	String GENE_TO_TISSUE_RESULT_NOT_FOUND_ERROR = "Gene to tissue record doesn't exist for given parameters...";
	String INVALID_TISSUE_NUMBER_ERROR = "Tissue number should be an integer...";
}
