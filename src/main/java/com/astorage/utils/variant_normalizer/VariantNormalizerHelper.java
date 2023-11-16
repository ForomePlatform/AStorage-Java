package com.astorage.utils.variant_normalizer;

import io.vertx.core.json.JsonObject;

public class VariantNormalizerHelper {
	public static JsonObject createNormalizedVariantJson(
		String refBuild,
		String chr,
		long pos,
		String ref,
		String alt
	) {
		return new JsonObject()
			.put("refBuild", refBuild)
			.put("chr", chr)
			.put("pos", pos)
			.put("ref", ref)
			.put("alt", alt);
	}
}
