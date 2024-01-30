package com.astorage.utils.gnomad;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Subgroup {
	public static final String[] SUBGROUPS = {
		"afr",
		"ami",
		"amr",
		"asj",
		"eas",
		"fin",
		"mid",
		"nfe",
		"sas",
		"remaining",
		"raw",
		"XY",
		"XX"
	};

	public static final String[] SUBGROUP_FIELDS = {
		"AC",
		"AN",
		"AF"
	};

	private final String subgroupName;
	private final Map<String, String> subgroupValues = new HashMap<>();

	public Subgroup(String subgroupName) {
		this.subgroupName = subgroupName;
	}

	public void putValue(String valueKey, String value) {
		subgroupValues.put(valueKey, value);
	}

	public JsonObject toJson() {
		JsonObject subgroupJson = new JsonObject();

		for (String field : SUBGROUP_FIELDS) {
			String valueKey = field + "_" + this.subgroupName;
			subgroupJson.put(valueKey, subgroupValues.get(valueKey));
		}

		return subgroupJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
