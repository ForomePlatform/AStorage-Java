package com.astorage.utils.variantNormalizer;

public interface VariantNormalizerConstants {
	// Normalization request params:
	String REF_BUILD_PARAM = "refBuild";
	String CHR_PARAM = "chr";
	String POS_PARAM = "pos";
	String REF_PARAM = "ref";
	String ALT_PARAM = "alt";

	// Error messages:
	String INVALID_CHR_OR_POS_ERROR = "Invalid 'chr' or 'pos'...";
	String REF_NOT_FOUND_ERROR = "Couldn't find the reference specified in the Fasta database...";
}
