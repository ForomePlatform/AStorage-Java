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

public class RocksDBRepository implements Constants {
	private final HashMap<String, ColumnFamilyHandle> columnFamilyHandleMap = new HashMap<>();
	private final String dbFilename;
	private final String dbDirectoryPath;
	private final RocksDB db;
	public final String dbName;

	public RocksDBRepository(String dbFilename, String dataDirectoryPath) throws RocksDBException, IOException {
		this.dbFilename = dbFilename;
		this.dbDirectoryPath = dataDirectoryPath + "/rocks-db";
		this.dbName = "RocksDB<" + dbFilename + ">";

		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

		try (
			DBOptions dbOptions = new DBOptions();
			ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
		) {
			// DBOptions config
			dbOptions.setCreateIfMissing(true);
			dbOptions.setCreateMissingColumnFamilies(true);

			File dbDir = new File(this.dbDirectoryPath, this.dbFilename);
			if (!dbDir.exists()) {
				Files.createDirectories(dbDir.getParentFile().toPath());
				Files.createDirectory(dbDir.getAbsoluteFile().toPath());

				columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
			} else {
				columnFamilyDescriptors.addAll(getColumnFamilyDescriptors(columnFamilyOptions));
			}

			db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);

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

	public synchronized void saveBytes(byte[] key, byte[] value) {
		try {
			db.put(key, value);
		} catch (RocksDBException e) {
			System.err.printf(
				"Error saving entry in RocksDB<%s>, cause: %s, message: %s%n",
				dbFilename,
				e.getCause(),
				e.getMessage()
			);
		}
	}

	public synchronized void saveBytes(byte[] key, byte[] value, ColumnFamilyHandle column) {
		try {
			db.put(column, key, value);
		} catch (RocksDBException e) {
			System.err.printf(
				"Error saving entry in RocksDB<%s>, cause: %s, message: %s%n",
				dbFilename,
				e.getCause(),
				e.getMessage()
			);
		}
	}

	public synchronized void saveString(byte[] key, String value) {
		saveBytes(key, value.getBytes());
	}

	public synchronized void saveString(byte[] key, String value, ColumnFamilyHandle column) {
		saveBytes(key, value.getBytes(), column);
	}

	public byte[] getBytes(byte[] key) {
		try {
			return db.get(key);
		} catch (RocksDBException e) {
			System.err.printf(
				"Error retrieving the entry in RocksDB<%s> from key: %s, cause: %s, message: %s%n",
				dbFilename,
				Arrays.toString(key),
				e.getCause(),
				e.getMessage()
			);
		}

		return null;
	}

	public byte[] getBytes(byte[] key, ColumnFamilyHandle column) {
		try {
			return db.get(column, key);
		} catch (RocksDBException e) {
			System.err.printf(
				"Error retrieving the entry in RocksDB<%s> from key: %s, cause: %s, message: %s%n",
				dbFilename,
				Arrays.toString(key),
				e.getCause(),
				e.getMessage()
			);
		}

		return null;
	}

	public String getString(byte[] key) {
		return new String(getBytes(key));
	}

	public String getString(byte[] key, ColumnFamilyHandle column) {
		return new String(getBytes(key, column));
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

	public ColumnFamilyHandle getColumnFamilyHandle(String name) {
		return columnFamilyHandleMap.get(name);
	}

	public void close() {
		db.close();
	}

	private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors(ColumnFamilyOptions columnFamilyOptions) {
		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

		try {
			File dbDir = new File(this.dbDirectoryPath, this.dbFilename);
			if (dbDir.exists()) {
				List<byte[]> columnFamilyByteNames = RocksDB.listColumnFamilies(new Options(), dbDir.getAbsolutePath());
				for (byte[] name : columnFamilyByteNames) {
					columnFamilyDescriptors.add(new ColumnFamilyDescriptor(name, columnFamilyOptions));
				}
			}

			return columnFamilyDescriptors;
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}
}
