package com.astorage.utils.dbsnp;

import com.astorage.utils.Constants;
import com.astorage.utils.JsonConvertible;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Variant implements JsonConvertible, DbSNPConstants {
	public static final String[] VARIANT_COLUMNS = {
		"REF",
		"ALT",
		"ID"
	};

	public final Map<String, String> variantColumnValues = new HashMap<>();

	public Variant(Map<String, Integer> columns, String[] row) throws Exception {
		for (String columnName : VARIANT_COLUMNS) {
			Integer columnIndex = columns.get(columnName);
			if (columnIndex == null) {
				throw new Exception(Constants.COLUMN_DOESNT_EXIST + columnName);
			}

			String columnValue = row[columnIndex];

			if (Objects.equals(columnValue, ".")) {
				variantColumnValues.put(columnName, null);
			} else {
				variantColumnValues.put(columnName, row[columns.get(columnName)]);
			}
		}
	}

	public JsonObject toJson() {
		JsonObject variantJson = new JsonObject();

		for (String column : VARIANT_COLUMNS) {
			variantJson.put(column, variantColumnValues.get(column));
		}

		return variantJson;
	}

	@Override
	public String toString() {
		return this.toJson().toString();
	}
}
