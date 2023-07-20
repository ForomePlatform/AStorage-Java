package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.clinvar.*;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("unused")
public class ClinVarIngestor implements Ingestor, Constants, ClinVarConstants {
	private final RoutingContext context;
	private final RocksDBRepository dbRep;

	public ClinVarIngestor(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();
		if (!(req.params().size() == 2
			&& req.params().contains(DATA_PATH_PARAM)
			&& req.params().contains(DATA_SUMMARY_PATH_PARAM))) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);
			return;
		}

		String dataPath = req.getParam(DATA_PATH_PARAM);
		String dataSummaryPath = req.getParam(DATA_SUMMARY_PATH_PARAM);

		storeXMLData(dataPath);
		storeVariantSummeryData(dataSummaryPath);

		String resp = "All Data has been ingested.\n";

		req.response()
			.putHeader("content-type", "text/plain")
			.end(resp);
	}

	private void storeXMLData(String dataPath) {
		ColumnFamilyHandle significanceColumnFamilyHandle = getOrCreateColumnFamily(SIGNIFICANCE_COLUMN_FAMILY_NAME);
		ColumnFamilyHandle submittersColumnFamilyHandle = getOrCreateColumnFamily(SUBMITTER_COLUMN_FAMILY_NAME);

		try {
			InputStream fileInputStream = new FileInputStream(dataPath);
			InputStream gzipInputStream = new GZIPInputStream(fileInputStream);

			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser parser = factory.newSAXParser();
			ClinVarXMLParser xmlParser = new ClinVarXMLParser();
			parser.parse(gzipInputStream, xmlParser);

			for (Significance significance : xmlParser.getSignificances()) {
				byte[] key = significance.getKey();
				byte[] compressedSignificance = Constants.compressJson(significance.toString());

				dbRep.saveBytes(key, compressedSignificance, significanceColumnFamilyHandle);
			}

			for (Submitter submitter : xmlParser.getSubmitters()) {
				byte[] key = submitter.getKey();
				byte[] compressedSubmitter = Constants.compressJson(submitter.toString());

				dbRep.saveBytes(key, compressedSubmitter, submittersColumnFamilyHandle);
			}
		} catch (ParserConfigurationException | SAXException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void storeVariantSummeryData(String dataSummaryPath) {
		ColumnFamilyHandle variatnsColumnFamilyHandle = getOrCreateColumnFamily(VARIANT_SUMMARY_COLUMN_FAMILY_NAME);

		try (
			InputStream fileInputStream = new FileInputStream(dataSummaryPath);
			InputStream gzipInputStream = new GZIPInputStream(fileInputStream);
			Reader decoder = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedReader = new BufferedReader(decoder)
		) {
			String line = bufferedReader.readLine();

			Map<String, Integer> columns;
			if (line == null || !line.startsWith(COLUMN_NAMES_LINE_PREFIX)) {
				Constants.errorResponse(context.request(), HttpURLConnection.HTTP_BAD_REQUEST, INVALID_FILE_CONTENT);

				return;
			}

			columns = Constants.mapColumns(line.substring(1), COLUMNS_DELIMITER);

			while ((line = bufferedReader.readLine()) != null) {
				String[] values = line.split(COLUMNS_DELIMITER);
				Variant variant = new Variant(columns, values);

				String alleleId = values[columns.get(ALLELE_ID_COLUMN_NAME)];
				byte[] key = alleleId.getBytes();
				byte[] compressedVariant = Constants.compressJson(variant.toString());

				dbRep.saveBytes(key, compressedVariant, variatnsColumnFamilyHandle);
			}

			bufferedReader.close();
		} catch (IOException e) {
			Constants.errorResponse(context.request(), HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private ColumnFamilyHandle getOrCreateColumnFamily(String columnFamilyName) {
		ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(columnFamilyName);
		if (columnFamilyHandle == null) columnFamilyHandle = dbRep.createColumnFamily(columnFamilyName);
		return columnFamilyHandle;
	}
}
