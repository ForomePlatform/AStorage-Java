package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.dbnsfp.*;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

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
			FileInputStream fileInputStream = new FileInputStream(file);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream))
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
			Variant lastVariant = null;
			while ((line = bufferedReader.readLine()) != null) {
				lastVariant = processLine(line, columns, lastVariant);
				lineCount++;
			}

			req.response()
				.putHeader("content-type", "text/json")
				.end(lineCount + " lines have been ingested!\n");
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

		dbRep.save(DbNSFPHelper.createKey(columns, row), variant.toString());

		return variant;
	}
}
