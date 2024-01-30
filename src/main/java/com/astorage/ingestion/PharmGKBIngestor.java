package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.pharmgkb.PharmGKBConstants;
import com.astorage.utils.pharmgkb.Variant;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

@SuppressWarnings("unused")
public class PharmGKBIngestor extends Ingestor implements PharmGKBConstants, Constants {
	public PharmGKBIngestor(
		RoutingContext context,
		RocksDBRepository dbRep,
		RocksDBRepository universalVariantDbRep,
		RocksDBRepository fastaDbRep
	) {
		super(context, dbRep, universalVariantDbRep, fastaDbRep);
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 2
				|| !req.params().contains(DATA_PATH_PARAM)
				|| !req.params().contains(DATA_TYPE_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String dataPath = req.getParam(DATA_PATH_PARAM);
		String dataType = req.getParam(DATA_TYPE_PARAM);

		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(dataType);
		if (columnFamilyHandle == null) {
			if (DATA_TYPES.contains(dataType)) {
				columnFamilyHandle = dbRep.createColumnFamily(dataType);
			} else {
				Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_DATA_TYPE_ERROR);

				return;
			}
		}

		File file = new File(dataPath);
		if (!file.exists()) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, FILE_NOT_FOUND_ERROR);

			return;
		}

		try {
			InputStream fileInputStream = new FileInputStream(file);
			Reader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

			storeData(dataType, bufferedReader, columnFamilyHandle);
		} catch (IOException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}

		Constants.successResponse(req, INGESTION_FINISH_MSG);
	}

	private void storeData(String dataType, BufferedReader reader, ColumnFamilyHandle columnFamilyHandle) throws IOException {
		// Skip the first row containing column names
		reader.readLine();
		String line;

		long lineCount = 0;
		while ((line = reader.readLine()) != null) {
			String[] values = line.strip().split(COLUMNS_DELIMITER);
			Variant variant = new Variant(dataType, values);
			byte[] compressedVariant = Constants.compressJson(variant.toString());

			dbRep.saveBytes(
				values[Variant.KEY_FIELD_INDEX].getBytes(),
				compressedVariant,
				columnFamilyHandle
			);

			lineCount++;

			Constants.logProgress(dbRep, lineCount, 100000);
		}

		reader.close();
	}
}
