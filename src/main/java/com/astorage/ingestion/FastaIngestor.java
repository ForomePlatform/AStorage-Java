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
public class FastaIngestor extends Ingestor implements Constants, FastaConstants {
	public FastaIngestor(
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
			req.params().size() != 3
				|| !req.params().contains(REF_BUILD_PARAM)
				|| !req.params().contains(DATA_PATH_PARAM)
				|| !req.params().contains(METADATA_PATH_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String refBuild = req.getParam(REF_BUILD_PARAM);
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

			System.out.println("Metadata read complete, starting ingestion...");

			long lineCount = storeData(refBuild, metadata, bufferedDataReader);

			Constants.successResponse(req, lineCount + " lines have been ingested in " + dbRep.dbName + "!");
		} catch (IOException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private long storeData(String refBuild, Map<String, String> metadata, BufferedReader reader) throws IOException {
		String line;
		String chr = null;
		long idx = 1;
		long lineCount = 0;

		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(refBuild);
		if (columnFamilyHandle == null) {
			columnFamilyHandle = dbRep.createColumnFamily(refBuild);
		}

		while ((line = reader.readLine()) != null) {
			if (line.startsWith(">")) {
				String refSeq = line.substring(1).split(" ")[0];
				chr = metadata.get(refSeq);

				if (chr.contains("chr")) {
					chr = chr.substring(3);
				}

				idx = 1;
			} else if (chr != null) {
				for (int i = 0; i < line.length(); i++) {
					dbRep.saveString(generateKey(chr, idx), String.valueOf(line.charAt(i)), columnFamilyHandle);
					idx++;
				}
			}

			lineCount++;

			Constants.logProgress(dbRep, lineCount, 100000);
		}

		return lineCount;
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

	public static byte[] generateKey(String chr, long idx) {
		byte[] a = chr.getBytes();
		byte[] b = ByteBuffer.allocate(8).putLong(idx).array();

		byte[] result = new byte[a.length + b.length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);

		return result;
	}
}
