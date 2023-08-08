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

	public Variant(Map<String, Integer> columns, String[] row) {
		for (String columnName : VARIANT_COLUMNS) {
			if (columns.get(columnName) == null) {
				System.err.println(Constants.COLUMN_DOESNT_EXIST + columnName);
			}

			String columnValue = row[columns.get(columnName)];

			if (Objects.equals(columnValue, ".")) {
				variantColumnValues.put(columnName, null);
			} else {
				variantColumnValues.put(columnName, row[columns.get(columnName)]);
			}
		}
	}

	public static byte[] generateKey(String chr, String pos) {
		return (chr + "_" + pos).getBytes();
	}

	public byte[] getKey() {
		String chr = variantColumnValues.get(CHR_COLUMN_NAME);
		String pos = variantColumnValues.get(POS_COLUMN_NAME);

		return generateKey(chr, pos);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (!(o instanceof Variant variant)) {
			return false;
		}

		return variantColumnValues.equals(variant.variantColumnValues);
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
