package com.astorage.utils.gtf;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Variant implements GTFConstants {
	public static final String[] VARIANT_COLUMNS = {
			"chromosome",
			"source",
			"feature",
			"start",
			"end",
			"score",
			"strand",
			"frame",
			"attributes"
	};
	public static final Map<String, String> CHOSEN_ATTRIBUTE_FIELDS = new HashMap<>() {{
		put("gene_name", "gene");
		put("gene_biotype", "biotype");
		put("exon_number", "exon");
		put("transcript_id", "transcript");
	}};

	public final Map<String, String> variantColumnValues = new HashMap<>();

	public Variant(List<String> values) {
		for (int i = 0; i < values.size(); i++) {
			if (i == values.size() - 1) {
				String[] attributes = values.get(i).split(ATTRIBUTES_DELIMITER);
				saveChosenAttributes(attributes);
			}

			variantColumnValues.put(VARIANT_COLUMNS[i], values.get(i));
		}
	}

	public static byte[] generateKey(String chr, String startPos, String endPos) {
		return (chr + "_" + startPos + "_" + endPos).getBytes();
	}

	private void saveChosenAttributes(String[] attributes) {
		for (String attribute : attributes) {
			String[] pair = attribute.strip().split(ATTRIBUTE_PAIR_DELIMITER);

			if (pair.length >= 2 && CHOSEN_ATTRIBUTE_FIELDS.containsKey(pair[0])) {
				variantColumnValues.put(CHOSEN_ATTRIBUTE_FIELDS.get(pair[0]), pair[1].strip());
			}
		}
	}

	public byte[] getKey() {
		String chr = variantColumnValues.get(CHROMOSOME_COLUMN_NAME);
		String startPos = variantColumnValues.get(START_POSITION_COLUMN_NAME);
		String endPos = variantColumnValues.get(END_POSITION_COLUMN_NAME);

		return generateKey(chr, startPos, endPos);
	}

	public JsonObject toJson() {
		JsonObject variantJson = new JsonObject();

		for (String column : VARIANT_COLUMNS) {
			variantJson.put(column, variantColumnValues.get(column));
		}

		for (String chosenAttributeFieldName : CHOSEN_ATTRIBUTE_FIELDS.values()) {
			variantJson.put(chosenAttributeFieldName, variantColumnValues.get(chosenAttributeFieldName));
		}

		return variantJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
