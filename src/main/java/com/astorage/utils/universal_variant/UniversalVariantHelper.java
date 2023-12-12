package com.astorage.utils.universal_variant;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;

public class UniversalVariantHelper {
	public static void ingestUniversalVariant(
		byte[] key,
		String variantQuery,
		String formatName,
		RocksDBRepository universalVariantDbRep
	) throws IOException {
		JsonArray variantQueries = new JsonArray();

		ColumnFamilyHandle columnFamilyHandle = universalVariantDbRep.getOrCreateColumnFamily(formatName);
		byte[] compressedVariantQueries = universalVariantDbRep.getBytes(key, columnFamilyHandle);

		if (compressedVariantQueries != null) {
			String decompressedVariantQueries = Constants.decompressJson(compressedVariantQueries);
			variantQueries = new JsonArray(decompressedVariantQueries);
		}

		variantQueries.add(variantQuery);
		byte[] compressedUpdatedVariantQueries = Constants.compressJson(variantQueries.toString());

		universalVariantDbRep.saveBytes(key, compressedUpdatedVariantQueries, columnFamilyHandle);
	}

	public static byte[] generateKey(JsonObject normalizedVariantJson) {
		return normalizedVariantJson.toString().getBytes();
	}
}
