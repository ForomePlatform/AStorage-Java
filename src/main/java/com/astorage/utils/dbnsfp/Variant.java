package com.astorage.utils.dbnsfp;

import com.astorage.utils.Constants;
import com.astorage.utils.JsonConvertible;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;

public class Variant implements JsonConvertible {
	public static final String[] VARIANT_COLUMNS = {
		"ref",
		"alt",
		"CADD_raw",
		"CADD_phred",
		"DANN_score",
		"DANN_rankscore",
		"Eigen-raw_coding",
		"Eigen-raw_coding_rankscore",
		"Eigen-phred_coding",
		"Eigen-PC-raw_coding",
		"Eigen-PC-raw_coding_rankscore",
		"Eigen-PC-phred_coding",
		"GTEx_V8_gene",
		"GTEx_V8_tissue",
		"Geuvadis_eQTL_target_gene"
	};

	public static final String VARIANT_ALT = "alt";

	public final Map<String, String> variantColumnValues = new HashMap<>();
	public final List<Facet> facets = new ArrayList<>();

	public Variant(Map<String, Integer> columns, String[] row) {
		for (String columnName : VARIANT_COLUMNS) {
			if (columns.get(columnName) == null) {
				System.err.println("Column does not exist: " + columnName);
			}

			String columnValue = row[columns.get(columnName)];

			if (Objects.equals(columnValue, ".")) {
				variantColumnValues.put(columnName, null);
			} else {
				variantColumnValues.put(columnName, row[columns.get(columnName)]);
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (!(o instanceof Variant variant)) {
			return false;
		}

		return variantColumnValues.equals(variant.variantColumnValues);
	}

	public JsonObject toJson() {
		JsonObject variantJson = new JsonObject();
		JsonArray facetsJson = Constants.listToJson(facets);

		for (String column : VARIANT_COLUMNS) {
			variantJson.put(column, variantColumnValues.get(column));
		}

		variantJson.put("facets", facetsJson);

		return variantJson;
	}

	@Override
	public String toString() {
		return this.toJson().toString();
	}
}
