package com.astorage.utils.dbsnp;

import java.util.Map;

public class DbSNPHelper implements DbSNPConstants {
	public static byte[] createKey(Map<String, Integer> columns, String[] row) {
		String chr = row[columns.get(CHR_COLUMN_NAME)];
		String pos = row[columns.get(POS_COLUMN_NAME)];

		return createKey(chr, pos);
	}

	public static byte[] createKey(String chr, String pos) {
		return (chr + "_" + pos).getBytes();
	}
}
