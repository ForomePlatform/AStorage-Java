package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.dbnsfp.*;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * For dbNSFP v4.3a!
 */
@SuppressWarnings("unused")
public class DbNSFPIngestor implements Ingestor, Constants, DbNSFPConstants {
	private final RoutingContext context;
	private final RocksDBRepository dbRep;

	public DbNSFPIngestor(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();

		if (!(req.params().size() == 1
			&& req.params().contains(DATA_PATH_PARAM))) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String dataPath = req.getParam(DATA_PATH_PARAM);

		File file = new File(dataPath);
		if (!file.exists()) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, FILE_NOT_FOUND_ERROR);

			return;
		}

		try (
			InputStream fileInputStream = new FileInputStream(file);
			InputStream gzipInputStream = new GZIPInputStream(fileInputStream);
			Reader decoder = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedReader = new BufferedReader(decoder)
		) {
			String line;
			Map<String, Integer> columns = null;

			if ((line = bufferedReader.readLine()) != null) {
				if (!line.startsWith(DbNSFPHelper.CHR_COLUMN_NAME)) {
					Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, "Invalid dbNSFP file...");

					return;
				}

				columns = mapColumns(line);
			}

			int lineCount = 0;
			byte[] lastKey = null;
			List<Variant> lastVariants = new ArrayList<>();

			while ((line = bufferedReader.readLine()) != null) {
				lastKey = processLine(line, columns, lastKey, lastVariants);
				lineCount++;

				if (lineCount % 10000 == 0) {
					System.out.println(dbRep.dbName + " progress: " + lineCount + " lines have been ingested...");
				}
			}

			if (!lastVariants.isEmpty()) {
				saveVariantsInDb(lastKey, lastVariants);
			}

			req.response()
				.putHeader("content-type", "text/json")
				.end(lineCount + " lines have been ingested in " + dbRep.dbName + "!\n");
		} catch (IOException e) {
			Constants.errorResponse(context.request(), HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private static Map<String, Integer> mapColumns(String line) {
		String[] columns = line.split(DATA_DELIMITER);
		Map<String, Integer> mappedColumns = new HashMap<>();

		for (int i = 0; i < columns.length; i++) {
			mappedColumns.put(columns[i], i);
		}

		return mappedColumns;
	}

	private byte[] processLine(String line, Map<String, Integer> columns, byte[] lastKey, List<Variant> lastVariants) {
		String[] row = line.split(DATA_DELIMITER);

		byte[] key = DbNSFPHelper.createKey(columns, row);
		Variant newVariant = new Variant(columns, row);
		Facet newFacet = new Facet(columns, row);
		List<Transcript> newTranscripts = Transcript.parseTranscripts(columns, row);

		newFacet.transcripts.addAll(newTranscripts);

		if (Arrays.equals(key, lastKey)) {
			Variant lastVariant = lastVariants.get(lastVariants.size() - 1);

			if (newVariant.equals(lastVariant)) {
				lastVariant.facets.add(newFacet);
			} else {
				newVariant.facets.add(newFacet);
				lastVariants.add(newVariant);
			}
		} else {
			if (!lastVariants.isEmpty()) {
				saveVariantsInDb(key, lastVariants);
			}

			newVariant.facets.add(newFacet);
			lastVariants.clear();
			lastVariants.add(newVariant);
		}

		return key;
	}

	private void saveVariantsInDb(byte[] key, List<Variant> variants) {
		JsonArray variantsJson = Constants.listToJson(variants);
		dbRep.save(key, variants.toString());
	}
}
