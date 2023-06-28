package com.astorage.utils.spliceai;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Variant implements SpliceAIConstants {
	public static final String[] VARIANT_COLUMNS = {
		"ALT",
		"REF",
		"ID"
	};
	public final Map<String, String> variantColumnValues = new HashMap<>();
	private final Info info;

	public Variant(Map<String, Integer> columns, String[] values, Map<String, Integer> infoFieldNamesToIndices) {
		for (String columnName : VARIANT_COLUMNS) {
			if (columns.get(columnName) == null) {
				System.err.println("Column does not exist: " + columnName);
			}

			variantColumnValues.put(columnName, values[columns.get(columnName)]);
		}

		String infoData = values[columns.get(INFO_COLUMN_NAME)];
		this.info = new Info(infoData, infoFieldNamesToIndices);
	}

	public JsonObject toJson() {
		JsonObject variantJson = new JsonObject();

		for (String column : VARIANT_COLUMNS) {
			variantJson.put(column, variantColumnValues.get(column));
		}

		this.info.addInfoToJsonObject(variantJson);

		return variantJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
