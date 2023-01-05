package com.astorage.db;

import org.rocksdb.ColumnFamilyHandle;

public interface KeyValueRepository<K, V> {
	void save(byte[] key, String value, ColumnFamilyHandle column);

	void save(K key, V value);

	V find(K key);

	String find(byte[] key, ColumnFamilyHandle column);
}
