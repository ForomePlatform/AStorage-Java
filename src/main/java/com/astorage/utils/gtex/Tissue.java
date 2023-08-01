package com.astorage.utils.gtex;

import com.astorage.utils.Constants;
import com.astorage.utils.JsonConvertible;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Tissue implements JsonConvertible, Constants, GTExConstants {
	public static final String[] TISSUE_COLUMNS = {
		TISSUE_NUMBER_KEY,
		"Name"
	};
	private final Map<String, String> tissueColumnValues = new HashMap<>();

	public Tissue(String number, String name) {
		tissueColumnValues.put(TISSUE_COLUMNS[0], number);
		tissueColumnValues.put(TISSUE_COLUMNS[1], name);
	}

	public static byte[] generateKey(String tissueNumber) {
		return tissueNumber.getBytes();
	}

	public byte[] getKey() {
		String tissueNumber = tissueColumnValues.get(TISSUE_NUMBER_KEY);

		return generateKey(tissueNumber);
	}

	public JsonObject toJson() {
		JsonObject tissueJson = new JsonObject();

		for (String column : TISSUE_COLUMNS) {
			tissueJson.put(column, tissueColumnValues.get(column));
		}

		return tissueJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
