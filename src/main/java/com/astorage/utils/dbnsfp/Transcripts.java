package com.astorage.utils.dbnsfp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Transcripts {
    public final List<Transcript> transcripts = new ArrayList<>();

    public Transcripts(Map<String, Integer> columns, String[] row) {
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
    }

    @Override
    public String toString() {
        return transcripts.toString();
    }
}
