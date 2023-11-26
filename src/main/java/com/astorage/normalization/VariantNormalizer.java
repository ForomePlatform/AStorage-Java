package com.astorage.normalization;

import com.astorage.db.RocksDBRepository;
import com.astorage.query.FastaQuery;
import com.astorage.utils.Constants;
import com.astorage.utils.fasta.FastaConstants;
import com.astorage.utils.variant_normalizer.VariantNormalizerConstants;
import com.astorage.utils.variant_normalizer.VariantNormalizerHelper;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.net.HttpURLConnection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Written according to GA4GH normalization technique.
 * Link: <a href="https://vrs.ga4gh.org/en/stable/impl-guide/normalization.html">...</a>
 * <p>
 * Uses the Fasta DB as a source for reference genome.
 */
public class VariantNormalizer implements Constants, VariantNormalizerConstants {
	protected final RoutingContext context;
	protected final RocksDBRepository dbRep;

	public VariantNormalizer(RoutingContext context, RocksDBRepository dbRep) throws IllegalArgumentException {
		if (dbRep.dbFormatName.equals(FastaConstants.FASTA_FORMAT_NAME.toLowerCase())) {
			throw new IllegalArgumentException(INCORRECT_DB_FORMAT);
		}

		this.context = context;
		this.dbRep = dbRep;
	}

	public void normalizationHandler() {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 5
				|| !req.params().contains(REF_BUILD_PARAM)
				|| !req.params().contains(CHR_PARAM)
				|| !req.params().contains(POS_PARAM)
				|| !req.params().contains(REF_PARAM)
				|| !req.params().contains(ALT_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String refBuild = req.getParam(REF_BUILD_PARAM);
		String chr = req.getParam(CHR_PARAM);
		String pos = req.getParam(POS_PARAM);
		String ref = req.getParam(REF_PARAM);
		String alt = req.getParam(ALT_PARAM);

		singleNormalizationHandler(refBuild, chr, pos, ref, alt, false);
	}

	protected void singleNormalizationHandler(
		String refBuild,
		String chr,
		String pos,
		String ref,
		String alt,
		boolean isBatched
	) {
		HttpServerRequest req = context.request();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_CHR_OR_POS_ERROR);

			return;
		}

		try {
			JsonObject result = normalizeVariant(refBuild, chr, pos, ref, alt, dbRep);

			if (isBatched) {
				req.response().write(result + "\n");
			} else {
				req.response()
					.putHeader("content-type", "application/json")
					.end(result + "\n");
			}
		} catch (Exception e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
		}
	}

	public static JsonObject normalizeVariant(
		String refBuild,
		String chr,
		String pos,
		String ref,
		String alt,
		RocksDBRepository dbRep
	) throws Exception {
		Pattern nucleotidePattern = Pattern.compile("^[" + NUCLEOTIDES + "]+$", Pattern.CASE_INSENSITIVE);
		Matcher matcher = nucleotidePattern.matcher(alt);
		boolean matchFound = matcher.find();

		if (!matchFound) {
			throw new Exception(GIVEN_ALT_NOT_SUPPORTED);
		}

		String modifiedChr = chr;
		if (modifiedChr.equals(MITOCHONDRIAL_CHR_ALT)) {
			modifiedChr = MITOCHONDRIAL_CHR;
		}

		try {
			String refFromFasta = FastaQuery.queryData(
				dbRep,
				refBuild,
				modifiedChr,
				Long.parseLong(pos),
				Long.parseLong(pos) + ref.length() - 1
			);

			if (refFromFasta == null || !refFromFasta.equals(ref)) {
				throw new Exception(REF_NOT_FOUND_ERROR);
			}
		} catch (InternalError e) {
			throw new Exception(e.getMessage());
		}

		if (ref.equals(alt)) {
			return VariantNormalizerHelper.createNormalizedVariantJson(
				refBuild,
				modifiedChr,
				Long.parseLong(pos),
				ref,
				alt
			);
		}

		int commonSuffixLength = commonAffixLength(ref, alt, false);
		String trimmedRef = ref.substring(0, ref.length() - commonSuffixLength);
		String trimmedAlt = alt.substring(0, alt.length() - commonSuffixLength);

		int commonPrefixLength = commonAffixLength(trimmedRef, trimmedAlt, true);
		trimmedRef = trimmedRef.substring(commonPrefixLength);
		trimmedAlt = trimmedAlt.substring(commonPrefixLength);

		long newPos = Long.parseLong(pos) + commonPrefixLength;

		try {
			if (!trimmedRef.isEmpty() && !trimmedAlt.isEmpty()) {
				return VariantNormalizerHelper.createNormalizedVariantJson(
					refBuild,
					modifiedChr,
					newPos,
					trimmedRef,
					trimmedAlt
				);
			} else if (trimmedAlt.isEmpty()) {
				String leftRollSequence = rollLeft(trimmedRef, newPos, refBuild, modifiedChr, dbRep);
				String rightRollSequence = rollRight(trimmedRef, newPos + trimmedRef.length(), refBuild, modifiedChr, dbRep);
				newPos -= leftRollSequence.length();

				return VariantNormalizerHelper.createNormalizedVariantJson(
					refBuild,
					modifiedChr,
					newPos,
					leftRollSequence + trimmedRef + rightRollSequence,
					leftRollSequence + rightRollSequence
				);
			} else {
				String leftRollSequence = rollLeft(trimmedAlt, newPos, refBuild, modifiedChr, dbRep);
				String rightRollSequence = rollRight(trimmedAlt, newPos, refBuild, modifiedChr, dbRep);
				newPos -= leftRollSequence.length();

				return VariantNormalizerHelper.createNormalizedVariantJson(
					refBuild,
					modifiedChr,
					newPos,
					leftRollSequence + rightRollSequence,
					leftRollSequence + trimmedAlt + rightRollSequence
				);
			}
		} catch (InternalError e) {
			throw new Exception(e.getMessage());
		}
	}

	private static int commonAffixLength(String ref, String alt, boolean calcPrefixLength) {
		int commonAffixLength = 0;

		int refLen = ref.length();
		int altLen = alt.length();
		int minLen = Math.min(refLen, altLen);

		int i = calcPrefixLength ? 0 : 1;
		if (calcPrefixLength) {
			while (i < minLen && ref.charAt(i) == alt.charAt(i)) {
				commonAffixLength++;
				i++;
			}

			return commonAffixLength;
		}

		while (i <= minLen && ref.charAt(refLen - i) == alt.charAt(altLen - i)) {
			commonAffixLength++;
			i++;
		}

		return commonAffixLength;
	}

	private static String rollLeft(
		String section,
		long startPos,
		String refBuild,
		String chr,
		RocksDBRepository dbRep
	) throws InternalError {
		StringBuilder result = new StringBuilder();

		long currPos = startPos - 1;
		String referenceNucleotide = FastaQuery.queryData(dbRep, refBuild, chr, currPos);

		int i = 0;
		while (Character.toString(section.charAt(section.length() - 1 - i)).equals(referenceNucleotide)) {
			result.append(referenceNucleotide);

			i++;
			i %= section.length();
			currPos--;

			referenceNucleotide = FastaQuery.queryData(dbRep, refBuild, chr, currPos);
		}

		return result.reverse().toString();
	}

	private static String rollRight(
		String section,
		long endPos,
		String refBuild,
		String chr,
		RocksDBRepository dbRep
	) throws InternalError {
		StringBuilder result = new StringBuilder();

		long currPos = endPos;
		String referenceNucleotide = FastaQuery.queryData(dbRep, refBuild, chr, currPos);

		int i = 0;
		while (Character.toString(section.charAt(i)).equals(referenceNucleotide)) {
			result.append(referenceNucleotide);

			i++;
			i %= section.length();
			currPos++;

			referenceNucleotide = FastaQuery.queryData(dbRep, refBuild, chr, currPos);
		}

		return result.toString();
	}
}
