package com.astorage.utils.gerp;

public class GERPHelper {
	public static byte[] createKey(String chr, String pos) {
		return (chr + "_" + pos).getBytes();
	}
}
