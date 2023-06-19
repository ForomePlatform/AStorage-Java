package com.astorage.utils.gnomad;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Subgroup {
	public static final String[] GROUPS = {
		"afr",
		"amr",
		"asj",
		"eas",
		"fin",
		"nfe",
		"sas",
		"oth",
		"raw",
		"male",
		"female"
	};
	public static final String[] SUBGROUP_FIELDS = {
		"AC",
		"AN",
		"AF"
	};

	public final Map<String, String> subgroupFieldValues = new HashMap<>();
	private String subgroupName = null;

	public Subgroup(String subgroupName) {
		this.subgroupName = subgroupName;
	}

	public JsonObject toJson() {
		io.vertx.core.json.JsonObject subgroupJson = new JsonObject();

		for (String field : SUBGROUP_FIELDS) {
			String fieldName = this.subgroupName + "_" + field;
			subgroupJson.put(fieldName, subgroupFieldValues.get(fieldName)); // ToDo: NULL
		}

		return subgroupJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
