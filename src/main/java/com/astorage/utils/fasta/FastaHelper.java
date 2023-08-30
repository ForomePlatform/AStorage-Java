package com.astorage.utils.fasta;

import com.astorage.db.RocksDBRepository;
import com.astorage.ingestion.FastaIngestor;
import org.rocksdb.ColumnFamilyHandle;

public class FastaHelper implements FastaConstants {
	public static String queryData(
		RocksDBRepository dbRep,
		String refBuild,
		String chr,
		long startPos,
		long endPos
	) throws InternalError {
		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(refBuild);

		if (columnFamilyHandle == null) {
			throw new InternalError(COLUMN_FAMILY_NULL_ERROR);
		}

		StringBuilder data = new StringBuilder();
		for (long i = startPos; i <= endPos; i++) {
			String retrievedData = dbRep.getString(FastaIngestor.generateKey(chr, i), columnFamilyHandle);

			if (retrievedData == null) {
				return null;
			}

			data.append(retrievedData);
		}

		return data.toString();
	}
}
