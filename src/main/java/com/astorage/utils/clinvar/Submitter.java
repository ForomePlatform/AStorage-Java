package com.astorage.utils.clinvar;

import com.astorage.utils.Constants;
import com.astorage.utils.JsonConvertible;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Submitter implements JsonConvertible, Constants, ClinVarConstants {
	public static final String[] SUBMITTER_COLUMNS = {
		"SubmitterID",
		"SubmitterName"
	};
	private final Map<String, String> submitterColumnValues = new HashMap<>();

	public static byte[] generateKey(String submitterId) {
		return submitterId.getBytes();
	}

	public byte[] getKey() {
		if (!submitterColumnValues.containsKey(SUBMITTER_ID_COLUMN_NAME)) {
			return null;
		}

		String submitterId = submitterColumnValues.get(SUBMITTER_ID_COLUMN_NAME);

		return generateKey(submitterId);
	}

	public void setSubmitterId(String id) {
		this.submitterColumnValues.put(SUBMITTER_COLUMNS[0], id);
	}

	public void setSubmitterName(String name) {
		this.submitterColumnValues.put(SUBMITTER_COLUMNS[1], name);
	}

	public JsonObject toJson() {
		JsonObject submitterJson = new JsonObject();

		for (String column : SUBMITTER_COLUMNS) {
			submitterJson.put(column, submitterColumnValues.get(column));
		}

		return submitterJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
