package com.astorage.main;

import com.astorage.db.RocksDBRepository;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class MainVerticle extends AbstractVerticle {
	private static final String ERROR_INVALID_PARAMS = "Error: Invalid parameters";
	private static final String ERROR_DOWNLOADING_DATA = "Error: Download failed";
	private static final String COLUMN_FAMILY_NULL = "Error: Array with the given name doesn't exist";
	private static final String METADATA_PATH = "/home/giorgi.shavtvalishvili/Quantori/AStorage/metadata";
	private static final String COMPRESSED_DATA_PATH = "/home/giorgi.shavtvalishvili/Quantori/AStorage/data.gz";
	private static final String DATA_PATH = "/home/giorgi.shavtvalishvili/Quantori/AStorage/data";
	private RocksDBRepository dbRep;

	@Override
	public void start(Promise<Void> startPromise) {
		Router router = Router.router(vertx);
		router.post("/ingestion/").handler(this::ingestionHandler);
		router.get("/query/").handler(this::queryHandler);

		vertx.createHttpServer().requestHandler(router).listen(8080, http -> {
			if (http.succeeded()) {
				startPromise.complete();
				System.out.println("HTTP server started on port 8080");
				dbRep = new RocksDBRepository();
				dbRep.initialize();
			} else {
				startPromise.fail(http.cause());
			}
		});
	}

	private void ingestionHandler(RoutingContext context) {
		HttpServerRequest req = context.request();
		if (!(req.params().contains("arrayName")
				&& req.params().contains("dataURL")
				&& req.params().contains("metadataURL"))) {
			errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, ERROR_INVALID_PARAMS);
			return;
		}

		String arrayName = req.getParam("arrayName");
		String dataURL = req.getParam("dataURL");
		String metadataURL = req.getParam("metadataURL");

		try {
//				downloadUsingStream(metadataURL, METADATA_PATH);
			Map<String, String> metadata = readMetadata();
//				downloadUsingStream(dataURL, COMPRESSED_DATA_PATH);
//				decompressGzip(COMPRESSED_DATA_PATH, DATA_PATH);
			storeData(arrayName, metadata);
		} catch (IOException e) {
			e.printStackTrace();
			errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, ERROR_DOWNLOADING_DATA);
			return;
		}

		String resp = "arrayName: " + arrayName + "\n" +
				"dataURL: " + dataURL + "\n" +
				"metadataURL: " + metadataURL + "\n";

		req.response()
				.putHeader("content-type", "text/plain")
				.end(resp);
	}

	private void queryHandler(RoutingContext context) {
		HttpServerRequest req = context.request();
		if (!(req.params().contains("arrayName")
				&& req.params().contains("sectionName")
				&& req.params().contains("startPosition")
				&& req.params().contains("endPosition"))) {
			errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, ERROR_INVALID_PARAMS);
			return;
		}

		String arrayName = req.getParam("arrayName");
		String sectionName = req.getParam("sectionName");
		int startPosition = Integer.parseInt(req.getParam("startPosition"));
		int endPosition = Integer.parseInt(req.getParam("endPosition"));

		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(arrayName);

		if (columnFamilyHandle == null) {
			errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, COLUMN_FAMILY_NULL);
			return;
		}

		StringBuilder stringBuilder = new StringBuilder();
		for (int i = startPosition; i <= endPosition; i++) {
			stringBuilder.append(dbRep.find(generateDBKey(sectionName, i), columnFamilyHandle));
		}

		req.response()
				.putHeader("content-type", "text/json")
				.end(
						new JsonObject()
								.put("array", arrayName)
								.put("section", sectionName)
								.put("start", startPosition)
								.put("end", endPosition)
								.put("data", stringBuilder.toString())
								.toString() + "\n"
				);
	}

	private static void downloadUsingStream(String urlStr, String file) throws IOException {
		URL url = new URL(urlStr);
		BufferedInputStream bufferedInputStream = new BufferedInputStream(url.openStream());
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		byte[] buffer = new byte[1024];
		int count;

		while ((count = bufferedInputStream.read(buffer, 0, 1024)) != -1) {
			fileOutputStream.write(buffer, 0, count);
		}

		bufferedInputStream.close();
		fileOutputStream.close();
	}

	private static Map<String, String> readMetadata() throws IOException {
		Map<String, String> result = new HashMap<>();
		BufferedReader reader = new BufferedReader(new FileReader(METADATA_PATH));
		String line;

		while ((line = reader.readLine()) != null) {
			if (!line.startsWith("#")) {
				String[] columns = line.split("\t");

				String seqName = columns[0];
				String refSeq = columns[6];
				String uscs = columns.length >= 10 ? columns[9] : "";

				result.put(refSeq, uscs.length() != 0 ? uscs : seqName);
			}
		}

		reader.close();

		return result;
	}

	private void storeData(String arrayName, Map<String, String> metadata) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(DATA_PATH));
		String line;
		String seqName = null;
		int idx = 1;

		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(arrayName);
		if (columnFamilyHandle == null) {
			columnFamilyHandle = dbRep.createColumnFamily(arrayName);
		}

		while ((line = reader.readLine()) != null) {
			if (line.startsWith(">")) {
				String refSeq = line.substring(1).split(" ")[0];
				seqName = metadata.get(refSeq);
				idx = 1;
			} else if (seqName != null) {
				for (int i = 0; i < line.length(); i++) {
					dbRep.save(generateDBKey(seqName, idx), line.charAt(i) + "", columnFamilyHandle);
					idx++;
				}
			}
		}

		reader.close();
	}

	private static byte[] generateDBKey(String seqName, int idx) {
		byte[] a = seqName.getBytes();
		byte[] b = ByteBuffer.allocate(4).putInt(idx).array();

		byte[] result = new byte[a.length + b.length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);

		return result;
	}

	public static void decompressGzip(String source, String target) throws IOException {
		try (
				GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(source));
				FileOutputStream fileOutputStream = new FileOutputStream(target)
		) {
			byte[] buffer = new byte[1024];
			int len;
			while ((len = gzipInputStream.read(buffer)) > 0) {
				fileOutputStream.write(buffer, 0, len);
			}
		}
	}

	private static void errorResponse(HttpServerRequest req, int errorCode, String errorMsg) {
		req.response()
				.setStatusCode(errorCode)
				.putHeader("content-type", "text/plain")
				.end(errorMsg + "\n");
	}
}
