package com.astorage.utils.dbnsfp;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.Objects;

public class DbNSFPHelper implements DbNSFPConstants {
	public static byte[] createKey(Map<String, Integer> columns, String[] row) {
		String chr = row[columns.get(CHR_COLUMN_NAME)];
		String pos = row[columns.get(POS_COLUMN_NAME)];

		return createKey(chr, pos);
	}

	public static byte[] createKey(String chr, String pos) {
		return (chr + "_" + pos).getBytes();
	}

	public static JsonArray processRawVariantsJson(JsonArray rawVariantsJson) {
		JsonArray variantsData = new JsonArray();

		for (int i = 0; i < rawVariantsJson.size(); i++) {
			JsonObject rawVariantJson = rawVariantsJson.getJsonObject(i);
			String rawVariantData = rawVariantJson.getString(COMPACTED_DATA_KEY);
			JsonObject variantData = processRawData(rawVariantData, Variant.VARIANT_COLUMNS);

			JsonArray facetsJson = new JsonArray();
			JsonArray rawFacetsJson = rawVariantJson.getJsonArray(FACETS_KEY);

			for (int j = 0; j < rawFacetsJson.size(); j++) {
				JsonObject rawFacetJson = rawFacetsJson.getJsonObject(j);
				String rawFacetData = rawFacetJson.getString(COMPACTED_DATA_KEY);
				JsonObject facetJson = processRawData(rawFacetData, Facet.FACET_COLUMNS);

				JsonArray transcriptsJson = new JsonArray();
				JsonArray rawTranscriptsJson = rawFacetJson.getJsonArray(TRANSCRIPTS_KEY);

				for (int k = 0; k < rawTranscriptsJson.size(); k++) {
					JsonObject rawTranscriptJson = rawTranscriptsJson.getJsonObject(k);
					String rawTranscriptData = rawTranscriptJson.getString(COMPACTED_DATA_KEY);
					JsonObject transcriptJson = processRawData(rawTranscriptData, Transcript.TRANSCRIPT_COLUMNS);

					transcriptsJson.add(transcriptJson);
				}

				facetJson.put(TRANSCRIPTS_KEY, transcriptsJson);
				facetsJson.add(facetJson);
			}

			variantData.put(FACETS_KEY, facetsJson);
			variantsData.add(variantData);
		}

		return variantsData;
	}

	private static JsonObject processRawData(String data, String[] columns) {
		JsonObject resultJson = new JsonObject();
		String[] values = data.split(DATA_DELIMITER);

		if (values.length != columns.length) {
			throw new RuntimeException(DB_DATA_NOT_CONSISTENT_ERROR);
		}

		for (int i = 0; i < values.length; i++) {
			if (Objects.equals(values[i], NULL_SHORTHAND)) {
				resultJson.put(columns[i], null);
			} else {
				resultJson.put(columns[i], values[i]);
			}
		}

		return resultJson;
	}
}
