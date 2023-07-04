package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.pharmgkb.PharmGKBConstants;
import com.astorage.utils.pharmgkb.Variant;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("unused")
public class PharmGKBIngestor implements Ingestor, PharmGKBConstants, Constants {
	private final RoutingContext context;
	private final RocksDBRepository dbRep;

	public PharmGKBIngestor(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();
		if (!(req.params().size() == 2
			&& req.params().contains(DATA_URL_PARAM)
			&& req.params().contains(DATA_TYPE_PARAM))
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String dataURL = req.getParam(DATA_URL_PARAM);
		String dataType = req.getParam(DATA_TYPE_PARAM);

		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(dataType);
		if (columnFamilyHandle == null) {
			if (DATA_TYPES.contains(dataType)) {
				columnFamilyHandle = dbRep.createColumnFamily(dataType);
			} else {
				JsonObject errorJson = new JsonObject();
				errorJson.put("error", INVALID_DATA_TYPE_ERROR);

				Constants.errorResponse(
					req,
					HttpURLConnection.HTTP_BAD_REQUEST,
					errorJson.toString()
				);

				return;
			}
		}

		try {
			Constants.downloadUsingStream(dataURL, COMPRESSED_DATA_FILENAME);
			File file = new File(DATA_DIRECTORY_PATH, COMPRESSED_DATA_FILENAME);
			InputStream fileInputStream = new FileInputStream(file);
			InputStream gzipInputStream = new GZIPInputStream(fileInputStream);
			Reader decoder = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedReader = new BufferedReader(decoder);

			storeData(dataType, bufferedReader, columnFamilyHandle);
		} catch (IOException | SecurityException | URISyntaxException e) {
			e.printStackTrace();
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, DOWNLOADING_DATA_ERROR);
			return;
		}

		String resp = "All Data has been ingested.\n";

		req.response()
			.putHeader("content-type", "text/plain")
			.end(resp);
	}

	private void storeData(String dataType, BufferedReader reader, ColumnFamilyHandle columnFamilyHandle) throws IOException {
		// Skip the first row containing column names
		reader.readLine();
		String line;

		while ((line = reader.readLine()) != null) {
			String[] values = line.strip().split(COLUMNS_DELIMITER);
			Variant variant = new Variant(dataType, values);

			dbRep.saveString(
				values[Variant.KEY_FIELD_INDEX].getBytes(),
				variant.toString(),
				columnFamilyHandle
			);
		}

		reader.close();
	}
}
