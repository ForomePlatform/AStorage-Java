package com.astorage.utils.dbnsfp;

import java.util.HashMap;
import java.util.Map;

public class Transcript {
    public static final String[] TRANSCRIPT_COLUMNS = {
            "Ensembl_transcriptid",
            "Ensembl_geneid",
            "Ensembl_proteinid",
            "refcodon",
            "codonpos",
            "FATHMM_score",
            "FATHMM_pred",
            "GENCODE_basic",
            "HGVSc_ANNOVAR",
            "HGVSp_ANNOVAR",
            "HGVSc_snpEff",
            "HGVSp_snpEff",
            "MPC_score",
            "MutationAssessor_score",
            "MutationAssessor_pred",
            "Polyphen2_HDIV_score",
            "Polyphen2_HDIV_pred",
            "Polyphen2_HVAR_score",
            "Polyphen2_HVAR_pred",
            "SIFT_score",
            "SIFT_pred",
            "SIFT4G_score",
            "SIFT4G_pred",
            "Uniprot_acc",
    };

    public final Map<String, String> transcriptColumnValues = new HashMap<>();

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("{");

        for (int i = 0; i < TRANSCRIPT_COLUMNS.length; i++) {
            String transcriptColumn = TRANSCRIPT_COLUMNS[i];
            String columnValue = transcriptColumnValues.get(transcriptColumn);
            result.append(DataStorage.columnToString(transcriptColumn, columnValue));

            if (i != TRANSCRIPT_COLUMNS.length - 1) {
                result.append(", ");
            }
        }

        result.append("}");

        return result.toString();
    }
}
