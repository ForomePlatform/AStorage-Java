package com.astorage.utils.gtex;

import com.astorage.utils.Constants;
import com.astorage.utils.JsonConvertible;
import io.vertx.core.json.JsonObject;
import javafx.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Gene implements JsonConvertible, Constants, GTExConstants {
	public static final String[] GENE_COLUMNS = {
		GENE_ID_KEY,
		SUB_ID_KEY,
		SYMBOL_KEY,
		"TopT1",
		"TopT2",
		"TopT3"
	};
	private final Map<String, String> geneColumnValues = new HashMap<>();

	public Gene(String[] keys, List<Pair<String, Double>> negExpressions) {
		for (int i = 0; i < 3; i++) {
			geneColumnValues.put(GENE_COLUMNS[i], keys[i]);
		}

		for (int i = 0; i < 3; i++) {
			String tissueNumber = null;
			Double negExpression = negExpressions.get(i).getValue();

			if (negExpression < 0) {
				tissueNumber = negExpressions.get(i).getKey();
			}

			geneColumnValues.put(GENE_COLUMNS[i + 3], tissueNumber);
		}
	}

	public static byte[] generateKey(String geneId, String subId) {
		return (geneId + "_" + subId).getBytes();
	}

	public byte[] getKey() {
		String geneId = geneColumnValues.get(GENE_ID_KEY);
		String subId = geneColumnValues.get(SUB_ID_KEY);

		return generateKey(geneId, subId);
	}

	public JsonObject toJson() {
		JsonObject geneJson = new JsonObject();

		for (String column : GENE_COLUMNS) {
			geneJson.put(column, geneColumnValues.get(column));
		}

		return geneJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
