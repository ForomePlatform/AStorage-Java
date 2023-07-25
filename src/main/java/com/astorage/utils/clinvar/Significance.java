package com.astorage.utils.clinvar;

import com.astorage.utils.Constants;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Significance implements Constants, ClinVarConstants {
	public static final String[] SIGNIFICANCE_COLUMNS = {
		"SubmitterID",
		"RCVaccession",
		"ClinicalSignificance"
	};
	private final Map<String, String> significanceColumnValues = new HashMap<>();

	public static byte[] generateKey(String rCVAccession) {
		return rCVAccession.getBytes();
	}

	public byte[] getKey() {
		String rCVAccession = significanceColumnValues.get(RCV_ACCESSION_COLUMN_NAME);

		return generateKey(rCVAccession);
	}

	public void setSubmitterId(String submitterId) {
		this.significanceColumnValues.put(SIGNIFICANCE_COLUMNS[0], submitterId);
	}

	public void setRCVAccession(String rcvAccession) {
		this.significanceColumnValues.put(SIGNIFICANCE_COLUMNS[1], rcvAccession);
	}

	public void setClinicalSignificance(String clinicalSignificance) {
		this.significanceColumnValues.put(SIGNIFICANCE_COLUMNS[2], clinicalSignificance);
	}

	public JsonObject toJson() {
		JsonObject significanceJson = new JsonObject();

		for (String column : SIGNIFICANCE_COLUMNS) {
			significanceJson.put(column, significanceColumnValues.get(column));
		}

		return significanceJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
