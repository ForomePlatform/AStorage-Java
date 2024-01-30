package com.astorage.utils.gnomad;

import com.astorage.utils.Constants;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Variant implements GnomADConstants {
	public static final String[] VARIANT_COLUMNS = {
		"POS",
		"ALT",
		"REF"
	};

	private final Info info;

	public final Map<String, String> variantColumnValues = new HashMap<>();

	public Variant(Map<String, Integer> columns, String[] values) throws Exception {
		for (String columnName : VARIANT_COLUMNS) {
			Integer columnIndex = columns.get(columnName);
			if (columnIndex == null) {
				throw new Exception(Constants.COLUMN_DOESNT_EXIST + columnName);
			}

			String columnValue = values[columnIndex];
			variantColumnValues.put(columnName, columnValue);
		}

		String infoData = values[columns.get(INFO_COLUMN_NAME)];
		this.info = new Info(infoData);
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
