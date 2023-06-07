package com.astorage.utils.gnomad;

public class GnomADHelper {
	public static byte[] createKey(String chr, String pos) {
		return (chr + "_" + pos).getBytes();
	}
}
