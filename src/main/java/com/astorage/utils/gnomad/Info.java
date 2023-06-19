package com.astorage.utils.gnomad;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Info implements GnomADConstants {
	public static final String[] INFO_FIELDS = {
		"AC",
		"AN",
		"AF",
		"nhomalt",
		"faf95",
		"faf99",
		"nhomalt_male",
	};

	public final Map<String, String> infoFieldValues = new HashMap<>();

	public Info(String info) {
		String[] delimitedInfo = info.split(INFO_FIELDS_DELIMITER);
		for (String infoPairString : delimitedInfo) {
			String[] infoPair = infoPairString.split(INFO_FIELD_KEY_VALUE_DELIMITER);
			if (infoPair.length != 2) {
				continue;
			}

			String key = infoPair[0];
			String value = infoPair[1];

			infoFieldValues.put(key, value);
		}
	}

	public Map<String, Subgroup> generateSubgroups() {
		Map<String, Subgroup> subgroups = new HashMap<>();
		for (String subgroupName : Subgroup.GROUPS) {
			for (String subgroupFieldName : Subgroup.SUBGROUP_FIELDS) {
				String subgroupFieldValue = infoFieldValues.get(subgroupName + "_" + subgroupFieldName);
				if (subgroupFieldValue == null) {
					continue;
				}

				Subgroup subgroup = subgroups.get(subgroupName);
				if (subgroup == null) {
					subgroup = subgroups.put(subgroupName, new Subgroup(subgroupName));
				}

				subgroup.subgroupFieldValues.put(subgroupFieldName, subgroupFieldValue);
			}
		}

		return subgroups;
	}

	public JsonObject toJson() {
		JsonObject infoJson = new JsonObject();

		for (String field : INFO_FIELDS) {
			infoJson.put(field, infoFieldValues.get(field));
		}

		return infoJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
