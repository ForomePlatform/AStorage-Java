package com.astorage.utils.pharmgkb;

import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class Variant implements PharmGKBConstants {
	public static final Map<String, String[]> VARIANT_COLUMNS = new HashMap<>() {{
		put(DATA_TYPES.get(0), new String[]{
			"GPID",
			"GTYPE",
			"CPTYPE"
		});
		put(DATA_TYPES.get(1), new String[]{
			"CAID",
			"LOC",
			"GEN",
			"LOE",
			"CAT",
			"AT",
			"VAIDS",
			"VA",
			"PMIDS",
			"EC",
			"RC",
			"RD",
			"RACE",
			"CHR"
		});
		put(DATA_TYPES.get(2), new String[]{
			"CAID_CAmeta ",
			"GPID_CA"
		});
		put(DATA_TYPES.get(3), new String[]{
			"SPID",
			"ST",
			"SC",
			"SCT",
			"CH",
			"CHT",
			"FIC",
			"AFCS",
			"FICT",
			"AFCT",
			"PVO",
			"PV",
			"RST",
			"RS",
			"CSTART",
			"CSTOP",
			"RACE"
		});
		put(DATA_TYPES.get(4), new String[]{
			"AID",
			"VAR",
			"GENE",
			"CHEM",
			"PMID",
			"PCAT",
			"SIGN",
			"NOTES",
			"SENT",
			"AL",
			"CHROM"
		});
		put(DATA_TYPES.get(5), new String[]{
			"AID_VDA ",
			"SPID_SPA"
		});
		put(DATA_TYPES.get(6), new String[]{
			"AID",
			"VAR",
			"GENE",
			"CHEM",
			"PMID",
			"PCAT",
			"SIGN",
			"NOTES",
			"SENT",
			"AL",
			"CHROM"
		});
		put(DATA_TYPES.get(7), new String[]{
			"AID_VFA ",
			"SPID_SPA"
		});
		put(DATA_TYPES.get(8), new String[]{
			"AID",
			"VAR",
			"GENE",
			"CHEM",
			"PMID",
			"PCAT",
			"SIGN",
			"NOTES",
			"SENT",
			"AL",
			"CHROM"
		});
		put(DATA_TYPES.get(9), new String[]{
			"AID_VPA ",
			"SPID_SPA"
		});
	}};
	public static int KEY_FIELD_INDEX = 0;
	private final String dataType;
	private final String[] values;

	public Variant(String dataType, String[] values) {
		this.dataType = dataType;
		this.values = values;
	}

	public JsonObject toJson() {
		JsonObject variantJson = new JsonObject();
		String[] variantColumns = VARIANT_COLUMNS.get(this.dataType);

		for (int i = 0; i < variantColumns.length; i++) {
			String columnName = variantColumns[i];
			variantJson.put(columnName, values[i]);
		}

		return variantJson;
	}

	public String toString() {
		return this.toJson().toString();
	}
}
