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
		"nhomalt_XY",
	};

	public final Map<String, String> infoFieldValues = new HashMap<>();
	public final Map<String, Subgroup> subgroups = new HashMap<>();

	public Info(String info) {
		String[] delimitedInfo = info.split(INFO_DELIMITER);

		for (String infoPairString : delimitedInfo) {
			String[] infoPair = infoPairString.split(INFO_PAIR_KEY_VALUE_DELIMITER);
			if (infoPair.length != 2) {
				continue;
			}

			infoFieldValues.put(infoPair[0], infoPair[1]);
		}

		this.generateSubgroups();
	}

	private void generateSubgroups() {
		for (String subgroupFieldName : Subgroup.SUBGROUP_FIELDS) {
			for (String subgroupName : Subgroup.SUBGROUPS) {
				String subgroupValueKey = subgroupFieldName + "_" + subgroupName;
				String subgroupValue = infoFieldValues.get(subgroupValueKey);
				if (subgroupValue == null) {
					continue;
				}

				Subgroup subgroup = subgroups.get(subgroupName);
				if (subgroup == null) {
					Subgroup newSubgroup = new Subgroup(subgroupName);
					subgroups.put(subgroupName, newSubgroup);
					subgroup = newSubgroup;
				}
				subgroup.putValue(subgroupValueKey, subgroupValue);
			}
		}
	}

	protected void addInfoToJsonObject(JsonObject jsonObject) {
		for (String fieldName : INFO_FIELDS) {
			jsonObject.put(fieldName, infoFieldValues.get(fieldName));
		}

		for (String subgroupName : this.subgroups.keySet()) {
			jsonObject.put(subgroupName, this.subgroups.get(subgroupName).toJson());
		}
	}
}
