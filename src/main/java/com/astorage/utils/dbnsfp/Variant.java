package com.astorage.utils.dbnsfp;

import java.util.*;

public class Variant {
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

    public static final String VARIANT_KEY = "alt";

    public final Map<String, String> variantColumnValues = new HashMap<>();
    // TODO: Do we really need a list here? For each key and alt there should only be one variant...
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

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("{");

        for (String variantColumn : VARIANT_COLUMNS) {
            String columnValue = variantColumnValues.get(variantColumn);
            result.append(DataStorage.columnToString(variantColumn, columnValue));
            result.append(", ");
        }

        result.append("\"facets\": ");
        result.append(facets);
        result.append("}");

        return result.toString();
    }
}
