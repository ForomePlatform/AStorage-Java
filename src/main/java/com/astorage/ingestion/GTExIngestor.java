package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.gtex.GTExConstants;
import com.astorage.utils.gtex.Gene;
import com.astorage.utils.gtex.GeneToTissue;
import com.astorage.utils.gtex.Tissue;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import javafx.util.Pair;
import org.rocksdb.ColumnFamilyHandle;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("unused")
public class GTExIngestor implements Ingestor, Constants, GTExConstants {
	private final RoutingContext context;
	private final RocksDBRepository dbRep;

	public GTExIngestor(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();

		if (req.params().size() != 1 || !req.params().contains(DATA_PATH_PARAM)) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		ColumnFamilyHandle geneColumnFamilyHandle = dbRep.getOrCreateColumnFamily(GENE_COLUMN_FAMILY_NAME);
		ColumnFamilyHandle tissueColumnFamilyHandle = dbRep.getOrCreateColumnFamily(TISSUE_COLUMN_FAMILY_NAME);
		ColumnFamilyHandle geneToTissueColumnFamilyHandle = dbRep.getOrCreateColumnFamily(GENE_TO_TISSUE_COLUMN_FAMILY_NAME);

		String dataPath = req.getParam(DATA_PATH_PARAM);

		try (
			InputStream fileInputStream = new FileInputStream(dataPath);
			InputStream gzipInputStream = new GZIPInputStream(fileInputStream);
			Reader decoder = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
			BufferedReader bufferedReader = new BufferedReader(decoder)
		) {
			String line;
			int lineNumber = 0;
			List<Gene> geneRecords = new ArrayList<>();
			List<GeneToTissue> geneToTissueRecords = new ArrayList<>();

			while ((line = bufferedReader.readLine()) != null) {
				lineNumber++;

				// First two lines are ignored
				if (lineNumber == 3) {
					String[] fields = line.split(COLUMNS_DELIMITER);

					ingestTissues(fields, tissueColumnFamilyHandle);
				} else if (lineNumber > 3) {
					String[] values = line.split(COLUMNS_DELIMITER);

					processValues(values, geneRecords, geneToTissueRecords);
				}
			}

			ingestGenes(geneRecords, geneColumnFamilyHandle);
			ingestGeneToTissues(geneToTissueRecords, geneToTissueColumnFamilyHandle);
		} catch (IOException e) {
			Constants.errorResponse(context.request(), HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());

			return;
		}

		String response = INGESTION_FINISH_MSG + "\n";

		req.response()
			.putHeader("content-type", "text/plain")
			.end(response);
	}

	private void processValues(String[] values, List<Gene> geneRecords, List<GeneToTissue> geneToTissueRecords) {
		List<Pair<String, Double>> negExpressions = new ArrayList<>();

		// First two values excluded
		for (int i = 2; i < values.length; i++) {
			String valueNumber = Integer.toString(i + 1);
			double negativeValue = -1 * Double.parseDouble(values[i]);

			Pair<String, Double> negExpression = new Pair<>(valueNumber, negativeValue);
			negExpressions.add(negExpression);
		}

		negExpressions.sort(Comparator.comparingDouble(Pair::getValue));
		double topExpression = -1 * negExpressions.get(0).getValue();

		if (topExpression <= 0) {
			return;
		}

		String[] identifiers = parseName(values[0]);
		String geneId = identifiers[0];
		String subId = identifiers.length > 1 ? identifiers[1] : "";
		String symbol = values[1];

		geneRecords.add(new Gene(
			new String[]{
				geneId,
				subId,
				symbol
			},
			negExpressions
		));

		for (Pair<String, Double> negExpression : negExpressions) {
			if (negExpression.getValue() >= 0) {
				return;
			}

			String tissueNo = negExpression.getKey();
			double expression = -1 * negExpression.getValue();

			geneToTissueRecords.add(new GeneToTissue(
				new String[]{
					geneId,
					subId,
					tissueNo,
					Double.toString(expression),
					Double.toString(expression / topExpression)
				}
			));
		}
	}

	private void ingestGenes(List<Gene> geneRecords, ColumnFamilyHandle geneColumnFamilyHandle) throws IOException {
		for (Gene gene : geneRecords) {
			byte[] key = gene.getKey();
			byte[] compressedGene = Constants.compressJson(gene.toString());

			dbRep.saveBytes(key, compressedGene, geneColumnFamilyHandle);
		}
	}

	private void ingestTissues(String[] fields, ColumnFamilyHandle tissueColumnFamilyHandle) throws IOException {
		// First two fields aren't representing tissues
		for (int i = 2; i < fields.length; i++) {
			String tissueNumber = Integer.toString(i + 1);
			String tissueName = fields[i];

			Tissue tissue = new Tissue(tissueNumber, tissueName);

			byte[] key = tissue.getKey();
			byte[] compressedTissue = Constants.compressJson(tissue.toString());

			dbRep.saveBytes(key, compressedTissue, tissueColumnFamilyHandle);
		}
	}

	private void ingestGeneToTissues(
		List<GeneToTissue> geneToTissueRecords,
		ColumnFamilyHandle geneToTissueColumnFamilyHandle
	) throws IOException {
		for (GeneToTissue geneToTissue : geneToTissueRecords) {
			byte[] key = geneToTissue.getKey();
			byte[] compressedGeneToTissue = Constants.compressJson(geneToTissue.toString());

			dbRep.saveBytes(key, compressedGeneToTissue, geneToTissueColumnFamilyHandle);
		}
	}

	private static String[] parseName(String id) {
		return id.split("\\.");
	}
}
