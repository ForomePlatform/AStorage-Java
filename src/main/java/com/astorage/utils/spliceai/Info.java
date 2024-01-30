package com.astorage.utils.spliceai;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class Info implements SpliceAIConstants {
	public static final String[] INFO_FIELDS = {
		"SYMBOL",
		"DP_AG",
		"DP_AL",
		"DP_DG",
		"DP_DL",
		"DS_AG",
		"DS_DG",
		"DS_AL",
		"DS_DL",
	};

	public static final Map<String, Function<Info, String>> INFO_ADDITIONAL_FIELDS = new HashMap<>() {{
		put("MAX_DS", (info) -> {
			float maxDS = 0;
			for (String field : Info.INFO_FIELDS) {
				if (field.startsWith(INFO_DS_FIELD_PREFIX)) {
					maxDS = Math.max(maxDS, Float.parseFloat(info.infoFieldValues.get(field)));
				}
			}

			return Float.toString(maxDS);
		});
	}};

	public final Map<String, String> infoFieldValues = new HashMap<>();

	public Info(String info, Map<String, Integer> infoFieldNamesToIndices) {
		String[] delimitedInfo = info.split(INFO_FIELDS_DELIMITER);

		for (String infoFieldName : INFO_FIELDS) {
			int fieldIndex = infoFieldNamesToIndices.get(infoFieldName);
			if (fieldIndex < delimitedInfo.length) {
				infoFieldValues.put(infoFieldName, delimitedInfo[fieldIndex]);
			}
		}
	}

	protected void addInfoToJsonObject(JsonObject jsonObject) {
		for (String fieldName : INFO_FIELDS) {
			jsonObject.put(
				fieldName,
				infoFieldValues.get(fieldName)
			);
		}

		for (String additionalFieldName : INFO_ADDITIONAL_FIELDS.keySet()) {
			jsonObject.put(
				additionalFieldName,
				INFO_ADDITIONAL_FIELDS.get(additionalFieldName).apply(this)
			);
		}
	}
}
