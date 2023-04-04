package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class FastaIngestor implements Ingestor, Constants, com.astorage.utils.fasta.Constants {
	private final RoutingContext context;
	private final RocksDBRepository dbRep;

	public FastaIngestor(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();
		if (!(req.params().size() == 3
			&& req.params().contains("arrayName")
			&& req.params().contains("dataURL")
			&& req.params().contains("metadataURL"))) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, ERROR_INVALID_PARAMS);
			return;
		}

		String arrayName = req.getParam("arrayName");
		String dataURL = req.getParam("dataURL");
		String metadataURL = req.getParam("metadataURL");

		try {
			downloadUsingStream(metadataURL, METADATA_FILENAME);
			Map<String, String> metadata = readMetadata();
			downloadUsingStream(dataURL, COMPRESSED_DATA_FILENAME);
			decompressGzip(COMPRESSED_DATA_FILENAME, DATA_FILENAME);
			storeData(arrayName, metadata);
		} catch (IOException | SecurityException e) {
			e.printStackTrace();
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, ERROR_DOWNLOADING_DATA);
			return;
		}

		String resp = "arrayName: " + arrayName + "\n" +
			"dataURL: " + dataURL + "\n" +
			"metadataURL: " + metadataURL + "\n";

		req.response()
			.putHeader("content-type", "text/plain")
			.end(resp);
	}

	private void storeData(String arrayName, Map<String, String> metadata) throws IOException {
		File dataFile = new File(DATA_DIRECTORY_PATH, DATA_FILENAME);
		BufferedReader reader = new BufferedReader(new FileReader(dataFile));
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

	private static void downloadUsingStream(String urlStr, String filename) throws IOException {
		File file = new File(DATA_DIRECTORY_PATH, filename);
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
		File metadataFile = new File(DATA_DIRECTORY_PATH, METADATA_FILENAME);
		BufferedReader reader = new BufferedReader(new FileReader(metadataFile));
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

	public static byte[] generateDBKey(String seqName, int idx) {
		byte[] a = seqName.getBytes();
		byte[] b = ByteBuffer.allocate(4).putInt(idx).array();

		byte[] result = new byte[a.length + b.length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);

		return result;
	}

	private static void decompressGzip(String source, String target) throws IOException {
		File sourceFile = new File(DATA_DIRECTORY_PATH, source);
		File targetFile = new File(DATA_DIRECTORY_PATH, target);

		try (
			GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(sourceFile));
			FileOutputStream fileOutputStream = new FileOutputStream(targetFile)
		) {
			byte[] buffer = new byte[1024];
			int len;
			while ((len = gzipInputStream.read(buffer)) > 0) {
				fileOutputStream.write(buffer, 0, len);
			}
		}
	}
}
