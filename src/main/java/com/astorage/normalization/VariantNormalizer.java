package com.astorage.normalization;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.fasta.FastaConstants;
import com.astorage.utils.fasta.FastaHelper;
import com.astorage.utils.variantNormalizer.VariantNormalizerConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.net.HttpURLConnection;

/**
 * Written according to GA4GH normalization technique.
 * Link: <a href="https://vrs.ga4gh.org/en/stable/impl-guide/normalization.html">...</a>
 *
 * Uses the Fasta DB as a source for reference genome.
 */
public class VariantNormalizer implements Normalizer, Constants, VariantNormalizerConstants {
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
		JsonObject errorJson = new JsonObject();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			errorJson.put(ERROR, INVALID_CHR_OR_POS_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		try {
			String refFromFasta = FastaHelper.queryData(
				dbRep,
				refBuild,
				chr,
				Long.parseLong(pos),
				Long.parseLong(pos) + ref.length() - 1
			);

			if (refFromFasta == null || !refFromFasta.equals(ref)) {
				errorJson.put(ERROR, REF_NOT_FOUND_ERROR);

				Constants.errorResponse(
					req,
					HttpURLConnection.HTTP_BAD_REQUEST,
					errorJson.toString()
				);

				return;
			}
		} catch (InternalError e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());

			return;
		}

		JsonObject result;

		if (ref.equals(alt)) {
			result = createNormalizedVariantJson(
				refBuild,
				chr,
				Long.parseLong(pos),
				ref,
				alt
			);
		} else {
			int commonSuffixLength = commonAffixLength(ref, alt, false);
			String trimmedRef = ref.substring(0, ref.length() - commonSuffixLength);
			String trimmedAlt = alt.substring(0, alt.length() - commonSuffixLength);

			int commonPrefixLength = commonAffixLength(trimmedRef, trimmedAlt, true);
			trimmedRef = trimmedRef.substring(commonPrefixLength);
			trimmedAlt = trimmedAlt.substring(commonPrefixLength);

			long newPos = Long.parseLong(pos) + commonPrefixLength;

			try {
				if (!trimmedRef.isEmpty() && !trimmedAlt.isEmpty()) {
					result = createNormalizedVariantJson(
						refBuild,
						chr,
						newPos,
						trimmedRef,
						trimmedAlt
					);
				} else if (trimmedAlt.isEmpty()) {
					String leftRollSequence = rollLeft(trimmedRef, newPos, refBuild, chr);
					String rightRollSequence = rollRight(trimmedRef, newPos + trimmedRef.length(), refBuild, chr);
					newPos -= leftRollSequence.length();

					result = createNormalizedVariantJson(
						refBuild,
						chr,
						newPos,
						leftRollSequence + trimmedRef + rightRollSequence,
						leftRollSequence + rightRollSequence
					);
				} else {
					String leftRollSequence = rollLeft(trimmedAlt, newPos, refBuild, chr);
					String rightRollSequence = rollRight(trimmedAlt, newPos, refBuild, chr);
					newPos -= leftRollSequence.length();

					result = createNormalizedVariantJson(
						refBuild,
						chr,
						newPos,
						leftRollSequence + rightRollSequence,
						leftRollSequence + trimmedAlt + rightRollSequence
					);
				}
			} catch (InternalError e) {
				Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());

				return;
			}
		}

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "text/json")
				.end(result + "\n");
		}
	}

	private int commonAffixLength(String ref, String alt, boolean calcPrefixLength) {
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

	private String rollLeft(String section, long startPos, String refBuild, String chr) throws InternalError {
		StringBuilder result = new StringBuilder();

		long currPos = startPos - 1;
		String referenceNucleotide = FastaHelper.queryData(dbRep, refBuild, chr, currPos);

		int i = 0;
		while (Character.toString(section.charAt(section.length() - 1 - i)).equals(referenceNucleotide)) {
			result.append(referenceNucleotide);

			i++;
			i %= section.length();
			currPos--;

			referenceNucleotide = FastaHelper.queryData(dbRep, refBuild, chr, currPos);
		}

        return result.reverse().toString();
    }

	private String rollRight(String section, long endPos, String refBuild, String chr) throws InternalError {
		StringBuilder result = new StringBuilder();

		long currPos = endPos;
		String referenceNucleotide = FastaHelper.queryData(dbRep, refBuild, chr, currPos);

		int i = 0;
		while (Character.toString(section.charAt(i)).equals(referenceNucleotide)) {
			result.append(referenceNucleotide);

			i++;
			i %= section.length();
			currPos++;

			referenceNucleotide = FastaHelper.queryData(dbRep, refBuild, chr, currPos);
		}

		return result.toString();
	}

	private JsonObject createNormalizedVariantJson(
		String refBuild,
		String chr,
		long pos,
		String ref,
		String alt
	) {
		return new JsonObject()
			.put("refBuild", refBuild)
			.put("chr", chr)
			.put("pos", pos)
			.put("ref", ref)
			.put("alt", alt);
	}
}
