package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.dbnsfp.DataStorage;
import com.astorage.utils.dbnsfp.Facet;
import com.astorage.utils.dbnsfp.Transcripts;
import com.astorage.utils.dbnsfp.Variant;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

/**
 * For dbNSFP v4.3a!
 */
public class DbNSFPIngestor implements Constants {
	public static final String DATA_DELIMITER = "\t";
	private final RoutingContext context;
	private final RocksDBRepository dbRep;

	public DbNSFPIngestor(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();
		if (!(req.params().size() == 1
			&& req.params().contains("fileName"))) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, ERROR_INVALID_PARAMS);
		}

		String filename = req.getParam("filename");

		File file = new File(filename);
		if (!file.exists()) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, "File does not exist: " + filename);
		}

		try (
			FileInputStream fileInputStream = new FileInputStream(file);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream))
		) {
			String line;
			Map<String, Integer> columns = null;

			if ((line = bufferedReader.readLine()) != null) {
				if (!line.startsWith(DataStorage.CHR_COLUMN_NAME)) {
					Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, "Invalid dbNSFP file...");
				}

				columns = mapColumns(line);
			}

			Variant lastVariant = null;
			while ((line = bufferedReader.readLine()) != null) {
				lastVariant = processLine(line, columns, lastVariant);
			}
		} catch (IOException e) {
			System.err.println("Internal error!");
			System.exit(1);
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

	private Variant processLine(String line, Map<String, Integer> columns, Variant lastVariant) {
		String[] row = line.split(DATA_DELIMITER);

		Variant variant = new Variant(columns, row);
		Facet facet = new Facet(columns, row);
		Transcripts transcripts = new Transcripts(columns, row);

		facet.transcripts.addAll(transcripts.transcripts);

		if (variant.equals(lastVariant)) {
			lastVariant.facets.add(facet);
			variant = lastVariant;
		} else {
			variant.facets.add(facet);
		}

		dbRep.save(DataStorage.createKey(columns, row), variant.toString());
		return variant;
	}
}
