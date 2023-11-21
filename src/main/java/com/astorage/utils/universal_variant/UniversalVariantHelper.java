package com.astorage.utils.universal_variant;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.Arrays;

public class UniversalVariantHelper {
	public static void ingestUniversalVariant(
		byte[] key,
		String variantQuery,
		String formatName,
		RocksDBRepository universalVariantDbRep
	) throws IOException {
		JsonObject variantQueries = new JsonObject();

		byte[] compressedVariantQueries = universalVariantDbRep.getBytes(key);

		if (compressedVariantQueries != null) {
			String decompressedVariantQueries = Constants.decompressJson(compressedVariantQueries);
			variantQueries = new JsonObject(decompressedVariantQueries);
		}

		int formatNameIndex = Arrays.asList(Constants.FORMAT_NAMES).indexOf(formatName);

		variantQueries.put(Integer.toString(formatNameIndex), variantQuery);
		byte[] compressedNewVariantQueries = Constants.compressJson(variantQueries.toString());

		universalVariantDbRep.saveBytes(key, compressedNewVariantQueries);
	}

	public static byte[] generateKey(JsonObject normalizedVariantJson) {
		return normalizedVariantJson.toString().getBytes();
	}
}
