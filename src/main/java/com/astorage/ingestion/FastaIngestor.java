package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.fasta.FastaConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("unused")
public class FastaIngestor implements Ingestor, Constants, FastaConstants {
	private final RoutingContext context;
	private final RocksDBRepository dbRep;

	public FastaIngestor(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 3
				|| !req.params().contains(ARRAY_NAME_PARAM)
				|| !req.params().contains(DATA_PATH_PARAM)
				|| !req.params().contains(METADATA_PATH_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String arrayName = req.getParam(ARRAY_NAME_PARAM);
		String dataPath = req.getParam(DATA_PATH_PARAM);
		String metadataPath = req.getParam(METADATA_PATH_PARAM);

		File dataFile = new File(dataPath);
		File metadataFile = new File(metadataPath);
		if (!dataFile.exists() || !metadataFile.exists()) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, FILE_NOT_FOUND_ERROR);

			return;
		}

		try (
			InputStream dataFileInputStream = new FileInputStream(dataFile);
			InputStream gzipInputStream = new GZIPInputStream(dataFileInputStream);
			Reader dataDecoder = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedDataReader = new BufferedReader(dataDecoder);

			InputStream metadataFileInputStream = new FileInputStream(metadataFile);
			Reader metadataReader = new InputStreamReader(metadataFileInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedMetadataReader = new BufferedReader(metadataReader)
		) {
			Map<String, String> metadata = readMetadata(bufferedMetadataReader);
			storeData(arrayName, metadata, bufferedDataReader);
		} catch (IOException e) {
			Constants.errorResponse(context.request(), HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());

			return;
		}

		String resp = "arrayName: " + arrayName + "\n" +
			"dataUrl: " + dataPath + "\n" +
			"metadataUrl: " + metadataPath + "\n";

		req.response()
			.putHeader("content-type", "text/plain")
			.end(resp);
	}

	private void storeData(String arrayName, Map<String, String> metadata, BufferedReader reader) throws IOException {
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
					dbRep.saveString(generateKey(seqName, idx), String.valueOf(line.charAt(i)), columnFamilyHandle);
					idx++;
				}
			}
		}
	}

	private static Map<String, String> readMetadata(BufferedReader reader) throws IOException {
		Map<String, String> result = new HashMap<>();
		String line;

		while ((line = reader.readLine()) != null) {
			if (!line.startsWith("#")) {
				String[] columns = line.split("\t");

				String seqName = columns[0];
				String refSeq = columns[6];
				String uscs = columns.length >= 10 ? columns[9] : "";

				result.put(refSeq, !uscs.isEmpty() ? uscs : seqName);
			}
		}

		return result;
	}

	public static byte[] generateKey(String seqName, long idx) {
		byte[] a = seqName.getBytes();
		byte[] b = ByteBuffer.allocate(8).putLong(idx).array();

		byte[] result = new byte[a.length + b.length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);

		return result;
	}
}
