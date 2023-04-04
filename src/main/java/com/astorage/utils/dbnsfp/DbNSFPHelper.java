package com.astorage.utils.dbnsfp;

import java.util.Map;

public class DbNSFPHelper implements DbNSFPConstants {
	public static byte[] createKey(Map<String, Integer> columns, String[] row) {
		String chr = row[columns.get(CHR_COLUMN_NAME)];
		String pos = row[columns.get(POS_COLUMN_NAME)];

		return (chr + "_" + pos).getBytes();
	}

	public static byte[] createKey(String chr, String pos) {
		return (chr + "_" + pos).getBytes();
	}

	public static String columnToString(String columnName, String columnValue) {
		StringBuilder result = new StringBuilder();

		result.append("\"").append(columnName).append("\"");
		result.append(": ");

		try {
			double parsedColumnValue = Double.parseDouble(columnValue);

			result.append(parsedColumnValue);
		} catch (NumberFormatException e) {
			result.append("\"").append(columnValue).append("\"");
		} catch (NullPointerException e) {
			result.append(columnValue);
		}

		return result.toString();
	}
}
