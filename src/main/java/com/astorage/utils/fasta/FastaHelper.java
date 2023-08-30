package com.astorage.utils.fasta;

import com.astorage.db.RocksDBRepository;
import com.astorage.ingestion.FastaIngestor;
import org.rocksdb.ColumnFamilyHandle;

public class FastaHelper implements FastaConstants {
	public static String queryData(
		RocksDBRepository dbRep,
		String arrayName,
		String sectionName,
		long startPos,
		long endPos
	) throws InternalError {
		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(arrayName);

		if (columnFamilyHandle == null) {
			throw new InternalError(COLUMN_FAMILY_NULL_ERROR);
		}

		StringBuilder data = new StringBuilder();
		for (long i = startPos; i <= endPos; i++) {
			data.append(dbRep.getString(FastaIngestor.generateKey(sectionName, i), columnFamilyHandle));
		}

		return data.toString();
	}
}
