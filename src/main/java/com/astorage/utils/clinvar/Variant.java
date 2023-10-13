package com.astorage.utils.clinvar;

import com.astorage.utils.Constants;
import com.astorage.utils.JsonConvertible;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Variant implements JsonConvertible, Constants, ClinVarConstants {
	public static final String[] VARIANT_COLUMNS = {
		"AlleleID",
		"Type",
		"Name",
		"GeneID",
		"GeneSymbol",
		"HGNC_ID",
		"ClinicalSignificance",
		"ClinSigSimple",
		"LastEvaluated",
		"RS# (dbSNP)",
		"nsv/esv (dbVar)",
		"RCVaccession",
		"PhenotypeIDS",
		"PhenotypeList",
		"Origin",
		"OriginSimple",
		"Assembly",
		"ChromosomeAccession",
		"Chromosome",
		"Start",
		"Stop",
		"ReferenceAllele",
		"AlternateAllele",
		"Cytogenetic",
		"ReviewStatus",
		"NumberSubmitters",
		"Guidelines",
		"TestedInGTR",
		"OtherIDs",
		"SubmitterCategories",
		"VariationID"
	};
	public final Map<String, String> variantColumnValues = new HashMap<>();

	public Variant(Map<String, Integer> columns, String[] row) {
		for (String columnName : VARIANT_COLUMNS) {
			if (columns.get(columnName) == null) {
				System.err.println(COLUMN_DOESNT_EXIST + columnName);
			} else {
				variantColumnValues.put(columnName, row[columns.get(columnName)]);
			}
		}
	}

	public static byte[] generateKey(String chr, String startPos, String endPos) {
		return (chr + "_" + startPos + "_" + endPos).getBytes();
	}

	public byte[] getKey() {
		String chr = variantColumnValues.get(CHROMOSOME_COLUMN_NAME);
		String startPos = variantColumnValues.get(START_POSITION_COLUMN_NAME);
		String endPos = variantColumnValues.get(END_POSITION_COLUMN_NAME);

		return generateKey(chr, startPos, endPos);
	}

	public JsonObject toJson() {
		JsonObject variantJson = new JsonObject();

		for (String column : VARIANT_COLUMNS) {
			variantJson.put(column, variantColumnValues.get(column));
		}

		return variantJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
