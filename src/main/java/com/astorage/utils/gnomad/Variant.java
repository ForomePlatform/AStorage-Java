package com.astorage.utils.gnomad;

import io.vertx.core.json.JsonObject;

import java.util.*;

public class Variant {
	public static final String[] VARIANT_COLUMNS = {
		"CHROM",
		"POS",
		"ALT",
		"REF"
	};

	private Map<String, Subgroup> subgroups = null;
	private Info info = null;

	public final Map<String, String> variantColumnValues = new HashMap<>();

	public Variant(Map<String, Integer> columns, String[] values) {
		for (String columnName : VARIANT_COLUMNS) {
			if (columns.get(columnName) == null) {
				System.err.println("Column does not exist: " + columnName);
			}

			variantColumnValues.put(columnName, values[columns.get(columnName)]);
		}
	}

	public void setInfo(Info info) {
		this.info = info;
	}

	public void setSubgroups(Map<String, Subgroup> subgroups) {
		this.subgroups = subgroups;
	}

	public JsonObject toJson() {
		JsonObject variantJson = new JsonObject();
		JsonObject infoJson = info == null ? new JsonObject() : info.toJson();
		JsonObject subgroupsJson = new JsonObject();

		for (String column : VARIANT_COLUMNS) {
			variantJson.put(column, variantColumnValues.get(column));
		}

		for (String key : subgroups.keySet()) {
			subgroupsJson.put(key, subgroups.get(key).toJson());
		}

		variantJson.put("info", infoJson);
		variantJson.put("subgroups", subgroupsJson);

		return variantJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
