package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.normalization.VariantNormalizer;
import com.astorage.utils.Constants;
import com.astorage.utils.clinvar.ClinVarConstants;
import com.astorage.utils.clinvar.Significance;
import com.astorage.utils.clinvar.Submitter;
import com.astorage.utils.clinvar.Variant;
import com.astorage.utils.universal_variant.UniversalVariantConstants;
import com.astorage.utils.universal_variant.UniversalVariantHelper;
import com.astorage.utils.variant_normalizer.VariantNormalizerConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("unused")
public class ClinVarIngestor extends Ingestor implements Constants, ClinVarConstants {
	private final ColumnFamilyHandle significanceColumnFamilyHandle;
	private final ColumnFamilyHandle submitterColumnFamilyHandle;
	private final ColumnFamilyHandle variatnsColumnFamilyHandle;

	// Variables for temporary data storage during XML parsing
	private Submitter lastSubmitter = new Submitter();
	private Significance lastSignificance = new Significance();
	private boolean isReferenceBlock = false;
	private boolean isClinicalSignificanceDescriptionBlock = false;

	// Reference build to be used during normalization
	private String refBuild;

	public ClinVarIngestor(
		RoutingContext context,
		RocksDBRepository dbRep,
		RocksDBRepository universalVariantDbRep,
		RocksDBRepository fastaDbRep
	) {
		super(context, dbRep, universalVariantDbRep, fastaDbRep);

		significanceColumnFamilyHandle = dbRep.getOrCreateColumnFamily(SIGNIFICANCE_COLUMN_FAMILY_NAME);
		submitterColumnFamilyHandle = dbRep.getOrCreateColumnFamily(SUBMITTER_COLUMN_FAMILY_NAME);
		variatnsColumnFamilyHandle = dbRep.getOrCreateColumnFamily(VARIANT_SUMMARY_COLUMN_FAMILY_NAME);
	}

	public void ingestionHandler() {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 3
				|| !req.params().contains(DATA_PATH_PARAM)
				|| !req.params().contains(DATA_SUMMARY_PATH_PARAM)
				|| !req.params().contains(VariantNormalizerConstants.REF_BUILD_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String dataPath = req.getParam(DATA_PATH_PARAM);
		String dataSummaryPath = req.getParam(DATA_SUMMARY_PATH_PARAM);
		this.refBuild = req.getParam(VariantNormalizerConstants.REF_BUILD_PARAM);

		storeXMLData(dataPath);
		storeVariantSummeryData(dataSummaryPath);

		Constants.successResponse(req, INGESTION_FINISH_MSG);
	}

	private void storeXMLData(String dataPath) {
		try {
			InputStream fileInputStream = new FileInputStream(dataPath);
			InputStream gzipInputStream = new GZIPInputStream(fileInputStream);

			XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
			XMLEventReader eventReader = xmlInputFactory.createXMLEventReader(gzipInputStream);

			while (eventReader.hasNext()) {
				XMLEvent nextEvent = eventReader.nextEvent();

				if (nextEvent.isStartElement()) {
					handleXMLStartElement(nextEvent.asStartElement());
				} else if (nextEvent.isCharacters()) {
					handleXMLCharacters(nextEvent.asCharacters());
				} else if (nextEvent.isEndElement()) {
					handleXMLEndElement(nextEvent.asEndElement());
				}
			}
		} catch (XMLStreamException | IOException e) {
			Constants.errorResponse(context.request(), HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private void handleXMLStartElement(StartElement startElement) {
		switch (startElement.getName().getLocalPart()) {
			case "ClinVarSet":
				this.lastSubmitter = new Submitter();
				this.lastSignificance = new Significance();
			case "ReferenceClinVarAssertion":
				this.isReferenceBlock = true;
				break;
			case "ClinVarAssertion":
				this.isReferenceBlock = false;
				break;
			case "ClinVarSubmissionID":
				Attribute submitterNameAttribute = startElement.getAttributeByName(new QName("submitter"));
				this.lastSubmitter.setSubmitterName(submitterNameAttribute.getValue());
				break;
			case "ClinVarAccession":
				if (this.isReferenceBlock) {
					Attribute rcvAccessionAttribute = startElement.getAttributeByName(new QName("Acc"));
					this.lastSignificance.setRCVAccession(rcvAccessionAttribute.getValue());
				} else {
					Attribute submitterIdAttribute = startElement.getAttributeByName(new QName("OrgID"));
					this.lastSignificance.setSubmitterId(submitterIdAttribute.getValue());
					this.lastSubmitter.setSubmitterId(submitterIdAttribute.getValue());
				}
				break;
			case "Description":
				if (!this.isReferenceBlock) {
					this.isClinicalSignificanceDescriptionBlock = true;
				}
		}
	}

	private void handleXMLCharacters(Characters characters) {
		if (this.isClinicalSignificanceDescriptionBlock) {
			this.lastSignificance.setClinicalSignificance(characters.getData());
		}
	}

	private void handleXMLEndElement(EndElement endElement) throws IOException {
		if (endElement.getName().getLocalPart().equals("ClinVarSet")) {
			byte[] significanceKey = lastSignificance.getKey();
			if (significanceKey != null) {
				byte[] compressedSignificance = Constants.compressJson(lastSignificance.toString());
				dbRep.saveBytes(significanceKey, compressedSignificance, significanceColumnFamilyHandle);
			}

			byte[] submitterKey = lastSubmitter.getKey();
			if (submitterKey != null) {
				byte[] compressedSubmitter = Constants.compressJson(lastSubmitter.toString());
				dbRep.saveBytes(submitterKey, compressedSubmitter, submitterColumnFamilyHandle);
			}
		}
	}

	private void storeVariantSummeryData(String dataSummaryPath) {
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
			List<Variant> lastVariants = new ArrayList<>();

			while ((line = bufferedReader.readLine()) != null) {
				String[] values = line.split(COLUMNS_DELIMITER);
				Variant variant = new Variant(columns, values);

				byte[] key = variant.getKey();
				byte[] compressedVariant = Constants.compressJson(variant.toString());

				ingestQueryParams(variant);

				dbRep.saveBytes(key, compressedVariant, variatnsColumnFamilyHandle);
			}
		} catch (Exception e) {
			Constants.errorResponse(context.request(), HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private void ingestQueryParams(Variant variant) throws Exception {
		if (this.refBuild.isEmpty()) {
			return;
		}

		String chr = variant.variantColumnValues.get(CHROMOSOME_COLUMN_NAME);
		String start_pos = variant.variantColumnValues.get(START_POSITION_COLUMN_NAME);
		String end_pos = variant.variantColumnValues.get(END_POSITION_COLUMN_NAME);
		String ref = variant.variantColumnValues.get(REF_COLUMN_NAME);
		String alt = variant.variantColumnValues.get(ALT_COLUMN_NAME);

		JsonObject normalizedVariantJson = VariantNormalizer.normalizeVariant(
			this.refBuild,
			chr,
			start_pos,
			ref,
			alt,
			fastaDbRep
		);

		byte[] universalVariantKey = UniversalVariantHelper.generateKey(normalizedVariantJson);

		// Param ordering should match query specification
		String variantQuery = String.join(
			UniversalVariantConstants.QUERY_PARAMS_DELIMITER,
			chr,
			start_pos,
			end_pos
		);

		UniversalVariantHelper.ingestUniversalVariant(
			universalVariantKey,
			variantQuery,
			CLINVAR_FORMAT_NAME,
			universalVariantDbRep
		);
	}
}
