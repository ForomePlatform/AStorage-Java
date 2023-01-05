package com.astorage.db;

import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RocksDBRepository implements KeyValueRepository<byte[], String> {
	private final static String DB_FILENAME = "a-storage";
	private final static String DB_DIR = "/tmp/rocks-db";
	private HashMap<String, ColumnFamilyHandle> columnFamilyHandleMap;
	private RocksDB db;

	public void initialize() {
		RocksDB.loadLibrary(); // ?
		final Options options = new Options();
		options.setCreateIfMissing(true);
		File dbDir = new File(DB_DIR, DB_FILENAME);
		columnFamilyHandleMap = new HashMap<>();

		List<ColumnFamilyDescriptor> columnFamilyDescriptors = getColumnFamilyDescriptors();
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

		try {
			Files.createDirectories(dbDir.getParentFile().toPath());
			Files.createDirectories(dbDir.getAbsoluteFile().toPath());

			db = RocksDB.open(new DBOptions(options), dbDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);

			for (ColumnFamilyHandle handle : columnFamilyHandles) {
				String name = new String(handle.getName());
				columnFamilyHandleMap.put(name, handle);
			}

		} catch (IOException | RocksDBException e) {
			System.out.printf(
					"Error initializing RocksDB, check configurations and permissions, exception: %s, message: %s, stackTrace: %s%n",
					e.getCause(),
					e.getMessage(),
					e.getStackTrace()
			);
		}

		System.out.println("RocksDB initialized and ready to use");
	}

	@Override
	public synchronized void save(byte[] key, String value) {
		try {
			db.put(key, value.getBytes());
		} catch (RocksDBException e) {
			System.out.printf(
					"Error saving entry in RocksDB, cause: %s, message: %s%n",
					e.getCause(),
					e.getMessage()
			);
		}
	}

	@Override
	public synchronized void save(byte[] key, String value, ColumnFamilyHandle column) {
		try {
			db.put(column, key, value.getBytes());
		} catch (RocksDBException e) {
			System.out.printf(
					"Error saving entry in RocksDB, cause: %s, message: %s%n",
					e.getCause(),
					e.getMessage()
			);
		}
	}

	@Override
	public String find(byte[] key) {
		String result = null;
		try {
			byte[] bytes = db.get(key);
			if (bytes == null) return null;
			result = new String(bytes);
		} catch (RocksDBException e) {
			System.out.printf(
					"Error retrieving the entry in RocksDB from key: %s, cause: %s, message: %s%n",
					key,
					e.getCause(),
					e.getMessage()
			);
		}

		return result;
	}

	@Override
	public String find(byte[] key, ColumnFamilyHandle column) {
		String result = null;
		try {
			byte[] bytes = db.get(column, key);
			if (bytes == null) return null;
			result = new String(bytes);
		} catch (RocksDBException e) {
			System.out.printf(
					"Error retrieving the entry in RocksDB from key: %s, cause: %s, message: %s%n",
					key,
					e.getCause(),
					e.getMessage()
			);
		}

		return result;
	}

	public synchronized ColumnFamilyHandle createColumnFamily(String name) {
		try {
			ColumnFamilyHandle handle = db.createColumnFamily(new ColumnFamilyDescriptor(name.getBytes()));
			columnFamilyHandleMap.put(name, handle);
			return handle;
		} catch (RocksDBException e) {
			System.out.printf(
					"Error creating column family in RocksDB, cause: %s, message: %s%n",
					e.getCause(),
					e.getMessage()
			);
		}

		return null;
	}

	private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors() {
		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
		try {
			File dbDir = new File(DB_DIR, DB_FILENAME);
			List<byte[]> columnFamilyByteNames = RocksDB.listColumnFamilies(new Options(), dbDir.getAbsolutePath());
			for (byte[] name : columnFamilyByteNames) {
				columnFamilyDescriptors.add(new ColumnFamilyDescriptor(name));
			}

			return columnFamilyDescriptors;
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	public ColumnFamilyHandle getColumnFamilyHandle(String name) {
		return columnFamilyHandleMap.get(name);
	}
}
