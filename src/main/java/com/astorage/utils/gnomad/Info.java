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

	public static Map<String, String> INFO_FIELD_ALTERNATIVE_NAMES = new HashMap<>() {{
		put("nhomalt_male", "hem");
	}};

	public final Map<String, String> infoFieldValues = new HashMap<>();
	public final Map<String, Subgroup> subgroups = new HashMap<>();

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

		this.generateSubgroups();
	}

	private void generateSubgroups() {
		for (String subgroupName : Subgroup.GROUPS) {
			for (String subgroupFieldName : Subgroup.SUBGROUP_FIELDS) {
				String subgroupFieldValue = infoFieldValues.get(subgroupName + "_" + subgroupFieldName);
				if (subgroupFieldValue == null) {
					continue;
				}

				Subgroup subgroup = this.subgroups.get(subgroupName);
				if (subgroup == null) {
					subgroup = this.subgroups.put(subgroupName, new Subgroup(subgroupName));
				}

				assert subgroup != null;
				subgroup.subgroupFieldValues.put(subgroupFieldName, subgroupFieldValue);
			}
		}
	}

	void addInfoToJsonObject(JsonObject jsonObject) {
		for (String fieldName : INFO_FIELDS) {
			jsonObject.put(
				INFO_FIELD_ALTERNATIVE_NAMES.getOrDefault(fieldName, fieldName),
				infoFieldValues.get(fieldName)
			);
		}

		for (String subgroupName : this.subgroups.keySet()) {
			jsonObject.put(subgroupName, this.subgroups.get(subgroupName).toJson());
		}
	}
}
