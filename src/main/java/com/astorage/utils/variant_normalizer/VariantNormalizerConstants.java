package com.astorage.utils.variant_normalizer;

public interface VariantNormalizerConstants {
	// General:
	String VARIANT_NORMALIZER_FORMAT_NAME = "VariantNormalizer";

	// Normalization request params:
	String REF_BUILD_PARAM = "refBuild";
	String NORMALIZE_PARAM = "normalize";
	String CHR_PARAM = "chr";
	String POS_PARAM = "pos";
	String REF_PARAM = "ref";
	String ALT_PARAM = "alt";

	// Error messages:
	String INVALID_CHR_OR_POS_ERROR = "Invalid 'chr' or 'pos'...";
	String REF_NOT_FOUND_ERROR = "Couldn't find the reference specified in the Fasta database...";
	String REF_NOT_EQUAL_ERROR = "Specified reference isn't equal to the reference from the Fasta database...";
	String GIVEN_REF_ALT_NOT_SUPPORTED = "Given ref/alt sequence isn't supported for normalization...";
	String INCORRECT_DB_FORMAT = "Normalizer tried initialization with an incorrect DB format...";
}
