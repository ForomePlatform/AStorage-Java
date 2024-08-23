package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.universal_variant.UniversalVariantConstants;
import com.astorage.utils.universal_variant.UniversalVariantHelper;
import com.astorage.utils.variant_normalizer.VariantNormalizerConstants;
import com.astorage.utils.variant_normalizer.VariantNormalizerHelper;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class UniversalVariantQuery implements Query, Constants, UniversalVariantConstants, VariantNormalizerConstants {
	protected final RoutingContext context;
	protected final RocksDBRepository dbRep;
	protected final Map<String, RocksDBRepository> dbRepositories;

	public UniversalVariantQuery(RoutingContext context, Map<String, RocksDBRepository> dbRepositories) {
		this.context = context;
		this.dbRep = dbRepositories.get(UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME);
		this.dbRepositories = dbRepositories;
	}

	public void queryHandler() throws IOException {
		HttpServerRequest req = context.request();

		if (
			req.params().size() != 5
				|| !req.params().contains(REF_BUILD_PARAM)
				|| !req.params().contains(CHR_PARAM)
				|| !req.params().contains(POS_PARAM)
				|| !req.params().contains(REF_PARAM)
				|| !req.params().contains(ALT_PARAM)
		) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);

			return;
		}

		String refBuild = req.getParam(REF_BUILD_PARAM);
		String chr = req.getParam(CHR_PARAM);
		String pos = req.getParam(POS_PARAM);
		String ref = req.getParam(REF_PARAM);
		String alt = req.getParam(ALT_PARAM);

		singleQueryHandler(refBuild, chr, pos, ref, alt, false);
	}

	protected void singleQueryHandler(
		String refBuild,
		String chr,
		String pos,
		String ref,
		String alt,
		boolean isBatched
	) throws IOException {
		HttpServerRequest req = context.request();
		chr = chr.toUpperCase();
		ref = ref.toUpperCase();
		alt = alt.toUpperCase();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr)) {
				Integer.parseInt(chr);
			}

			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_CHR_OR_POS_ERROR);

			return;
		}

		JsonObject normalizedVariantJson = VariantNormalizerHelper.createNormalizedVariantJson(
			refBuild,
			chr,
			Long.parseLong(pos),
			ref,
			alt
		);
		byte[] key = UniversalVariantHelper.generateKey(normalizedVariantJson);

		JsonObject result = new JsonObject();
		for (String formatName : UNIVERSAL_QUERY_FORMAT_NAMES) {
			try {
				// Taking class
				Class<?> queryClass = Class.forName("com.astorage.query." + formatName + "Query");
				// Checking if it overrides normalizedParamsToParams
				Class<?>[] methodParamTypes = new Class<?>[QUERY_PARAMS_COUNT];
				Arrays.fill(methodParamTypes, String.class);
				Method normalizedParamsToParams = queryClass.getMethod("normalizedParamsToParams", methodParamTypes);
				String[] queryParams = (String[]) normalizedParamsToParams.invoke(null, refBuild, chr, pos, ref, alt);

				List<String[]> queryParamsList = new ArrayList<>();

				// If normalizedParamsToParams isn't overridden for a specific query class
				if (queryParams == null) {
					ColumnFamilyHandle columnFamilyHandle = dbRep.getColumnFamilyHandle(formatName);
					// No stored variant queries for this format
					if (columnFamilyHandle == null) {
						result.put(formatName, new JsonArray());
						continue;
					}

					byte[] compressedVariantQueries = dbRep.getBytes(key, columnFamilyHandle);
					// No stored variant queries for this normalized query
					if (compressedVariantQueries == null) {
						result.put(formatName, new JsonArray());
						continue;
					}

					String decompressedVariantQueries = Constants.decompressJson(compressedVariantQueries);
					JsonArray variantQueries = new JsonArray(decompressedVariantQueries);

					for (Object variantQueryObject : variantQueries) {
						String variantQuery = (String) variantQueryObject;
						// Store query params
						queryParamsList.add(variantQuery.split(QUERY_PARAMS_DELIMITER));
					}
				} else {
					queryParamsList.add(queryParams);
				}

				for (String[] currQueryParams : queryParamsList) {
					// Get query results from the specific query class
					JsonObject currQueryResult = queryFormatData(queryClass, formatName, currQueryParams);

					if (result.getJsonArray(formatName) == null) {
						result.put(formatName, new JsonArray());
					}

					result.getJsonArray(formatName).add(currQueryResult);
				}
			} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException ignored) {
				// Ignores exceptions that are thrown when a variant isn't found during queryData invocation
				// inside the queryFormatData method
				result.put(formatName, new JsonArray());
			}
		}

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "application/json")
				.end(result + "\n");
		}
	}

	private JsonObject queryFormatData(
		Class<?> queryClass,
		String formatName,
		String[] queryParams
	) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Class<?>[] queryParamTypes = new Class<?>[queryParams.length + 1];
		queryParamTypes[0] = RocksDBRepository.class;
		Arrays.fill(queryParamTypes, 1, queryParams.length + 1, String.class);

		RocksDBRepository dbRep = dbRepositories.get(formatName);

		List<Object> queryParamsWithDBRep = new ArrayList<>();
		queryParamsWithDBRep.add(dbRep);
		queryParamsWithDBRep.addAll(Arrays.asList(queryParams));

		Method queryDataMethod = queryClass.getDeclaredMethod("queryData", queryParamTypes);

		return (JsonObject) queryDataMethod.invoke(null, queryParamsWithDBRep.toArray());
	}
}
