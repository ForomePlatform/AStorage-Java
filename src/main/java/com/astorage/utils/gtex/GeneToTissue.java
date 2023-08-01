package com.astorage.utils.gtex;

import com.astorage.utils.Constants;
import com.astorage.utils.JsonConvertible;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class GeneToTissue implements JsonConvertible, Constants, GTExConstants {
	public static final String[] GENE_TO_TISSUE_COLUMNS = {
		GENE_ID_KEY,
		SUB_ID_KEY,
		TISSUE_NUMBER_KEY,
		"Expression",
		"RelExp"
	};
	private final Map<String, String> geneToTissueColumnValues = new HashMap<>();

	public GeneToTissue(String[] geneToTissueData) {
		for (int i = 0; i < GENE_TO_TISSUE_COLUMNS.length; i++) {
			geneToTissueColumnValues.put(GENE_TO_TISSUE_COLUMNS[i], geneToTissueData[i]);
		}
	}

	public static byte[] generateKey(String geneId, String subId, String tissueNo) {
		return (geneId + "_" + subId + "_" + tissueNo).getBytes();
	}

	public byte[] getKey() {
		String geneId = geneToTissueColumnValues.get(GENE_ID_KEY);
		String subId = geneToTissueColumnValues.get(SUB_ID_KEY);
		String tissueNo = geneToTissueColumnValues.get(TISSUE_NUMBER_KEY);

		return generateKey(geneId, subId, tissueNo);
	}

	public JsonObject toJson() {
		JsonObject geneToTissueJson = new JsonObject();

		for (String column : GENE_TO_TISSUE_COLUMNS) {
			geneToTissueJson.put(column, geneToTissueColumnValues.get(column));
		}

		return geneToTissueJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
