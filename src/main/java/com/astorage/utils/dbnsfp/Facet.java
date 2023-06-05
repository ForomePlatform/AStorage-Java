package com.astorage.utils.dbnsfp;

import com.astorage.utils.Constants;
import com.astorage.utils.JsonConvertible;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;

public class Facet implements JsonConvertible {
	public static final String[] FACET_COLUMNS = {
		"CADD_raw_rankscore",
		"MetaLR_score",
		"MetaLR_rankscore",
		"MetaLR_pred",
		"MutPred_score",
		"MutPred_rankscore",
		"MutPred_protID",
		"MutPred_AAchange",
		"MutPred_Top5features",
		"MPC_rankscore",
		"PrimateAI_score",
		"PrimateAI_rankscore",
		"PrimateAI_pred",
		"REVEL_score",
		"SIFT_converted_rankscore",
		"SIFT4G_converted_rankscore",
		"MutationTaster_score",
		"MutationTaster_pred"
	};

	public final Map<String, String> facetColumnValues = new HashMap<>();
	public final List<Transcript> transcripts = new ArrayList<>();

	public Facet(Map<String, Integer> columns, String[] row) {
		for (String columnName : FACET_COLUMNS) {
			if (columns.get(columnName) == null) {
				System.err.println("Column does not exist: " + columnName);
			}

			String columnValue = row[columns.get(columnName)];

			if (Objects.equals(columnValue, ".")) {
				facetColumnValues.put(columnName, null);
			} else {
				facetColumnValues.put(columnName, row[columns.get(columnName)]);
			}
		}
	}

	public JsonObject toJson() {
		JsonObject facetJson = new JsonObject();
		JsonArray transcriptsJson = Constants.listToJson(transcripts);

		for (String column : FACET_COLUMNS) {
			facetJson.put(column, facetColumnValues.get(column));
		}

		facetJson.put("transcripts", transcriptsJson);

		return facetJson;
	}

	@Override
	public String toString() {
		return this.toJson().toString();
	}
}
