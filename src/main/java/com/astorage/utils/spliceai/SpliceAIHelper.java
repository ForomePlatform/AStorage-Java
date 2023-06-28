package com.astorage.utils.spliceai;

public class SpliceAIHelper {
	public static byte[] createKey(String chr, String pos) {
		return (chr + "_" + pos).getBytes();
	}
}
