package com.astorage.utils.dbnsfp;

import com.astorage.utils.JsonConvertible;
import io.vertx.core.json.JsonObject;

import java.util.*;

public class Transcript implements JsonConvertible {
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

	public static List<Transcript> parseTranscripts(Map<String, Integer> columns, String[] row) {
		List<Transcript> transcripts = new ArrayList<>();

		for (String columnName : Transcript.TRANSCRIPT_COLUMNS) {
			if (columns.get(columnName) == null) {
				System.err.println("Column does not exist: " + columnName);
			}

			String columnValues = row[columns.get(columnName)];
			String[] splitColumnValues = columnValues.split(";");

			for (int i = 0; i < splitColumnValues.length; i++) {
				String columnValue = null;

				if (!Objects.equals(splitColumnValues[i], ".")) {
					columnValue = splitColumnValues[i];
				}

				if (transcripts.size() <= i) {
					Transcript transcript = new Transcript();
					transcript.transcriptColumnValues.put(columnName, columnValue);
					transcripts.add(transcript);
				} else {
					transcripts.get(i).transcriptColumnValues.put(columnName, columnValue);
				}
			}
		}

		return transcripts;
	}

	public JsonObject toJson() {
		JsonObject transcriptJson = new JsonObject();

		StringBuilder dataBuilder = new StringBuilder();
		for (int i = 0; i < TRANSCRIPT_COLUMNS.length; i++) {
			String columnValue = transcriptColumnValues.get(TRANSCRIPT_COLUMNS[i]);
			dataBuilder.append(Objects.requireNonNullElse(columnValue, DbNSFPConstants.NULL_SHORTHAND));

			if (i < TRANSCRIPT_COLUMNS.length - 1) {
				dataBuilder.append(DbNSFPConstants.DATA_DELIMITER);
			}
		}

		transcriptJson.put(DbNSFPConstants.COMPACTED_DATA_KEY, dataBuilder.toString());

		return transcriptJson;
	}

	@Override
	public String toString() {
		return this.toJson().toString();
	}
}
