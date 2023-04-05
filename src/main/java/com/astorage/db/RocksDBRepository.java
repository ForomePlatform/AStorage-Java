package com.astorage.db;

import com.astorage.utils.Constants;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class RocksDBRepository implements KeyValueRepository<byte[], String>, Constants {
	private final static String DB_DIR = DATA_DIRECTORY_PATH + "/rocks-db";
	private final HashMap<String, ColumnFamilyHandle> columnFamilyHandleMap = new HashMap<>();
	private final String dbFilename;
	private final RocksDB db;
	public final String dbName;

	public RocksDBRepository(String dbFilename) throws RocksDBException, IOException {
		this.dbFilename = dbFilename;
		this.dbName = "RocksDB<" + dbFilename + ">";

		final Options options = new Options()
			.setCreateIfMissing(true)
			.setCreateMissingColumnFamilies(true);

		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

		try {
			File dbDir = new File(DB_DIR, this.dbFilename);
			if (!dbDir.exists()) {
				Files.createDirectories(dbDir.getParentFile().toPath());
				Files.createDirectory(dbDir.getAbsoluteFile().toPath());

				columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
			}

			columnFamilyDescriptors.addAll(getColumnFamilyDescriptors());

			db = RocksDB.open(new DBOptions(options), dbDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);

			for (ColumnFamilyHandle handle : columnFamilyHandles) {
				String name = new String(handle.getName());
				columnFamilyHandleMap.put(name, handle);
			}
		} catch (IOException | RocksDBException e) {
			System.err.printf(
				"Error initializing RocksDB<%s>, check configurations and permissions, exception: %s, message: %s, stackTrace: %s%n",
				dbFilename,
				e.getCause(),
				e.getMessage(),
				Arrays.toString(e.getStackTrace())
			);

			throw e;
		}

		System.out.printf("RocksDB<%s> initialized and ready to use%n", dbFilename);
	}

	@Override
	public synchronized void save(byte[] key, String value) {
		try {
			db.put(key, value.getBytes());
		} catch (RocksDBException e) {
			System.err.printf(
				"Error saving entry in RocksDB<%s>, cause: %s, message: %s%n",
				dbFilename,
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
			System.err.printf(
				"Error saving entry in RocksDB<%s>, cause: %s, message: %s%n",
				dbFilename,
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
			System.err.printf(
				"Error retrieving the entry in RocksDB<%s> from key: %s, cause: %s, message: %s%n",
				dbFilename,
				Arrays.toString(key),
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
			System.err.printf(
				"Error retrieving the entry in RocksDB<%s> from key: %s, cause: %s, message: %s%n",
				dbFilename,
				Arrays.toString(key),
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
			System.err.printf(
				"Error creating column family in RocksDB<%s>, cause: %s, message: %s%n",
				dbFilename,
				e.getCause(),
				e.getMessage()
			);
		}

		return null;
	}

	private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors() {
		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

		try {
			File dbDir = new File(DB_DIR, this.dbFilename);
			if (dbDir.exists()) {
				List<byte[]> columnFamilyByteNames = RocksDB.listColumnFamilies(new Options(), dbDir.getAbsolutePath());
				for (byte[] name : columnFamilyByteNames) {
					columnFamilyDescriptors.add(new ColumnFamilyDescriptor(name));
				}
			}

			return columnFamilyDescriptors;
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	public ColumnFamilyHandle getColumnFamilyHandle(String name) {
		return columnFamilyHandleMap.get(name);
	}

	public void close() {
		db.close();
	}
}
