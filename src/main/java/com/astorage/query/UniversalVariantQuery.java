package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.universal_variant.UniversalVariantConstants;
import com.astorage.utils.universal_variant.UniversalVariantHelper;
import com.astorage.utils.variant_normalizer.VariantNormalizerConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

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

	private void singleQueryHandler(
		String refBuild,
		String chr,
		String pos,
		String ref,
		String alt,
		boolean isBatched
	) throws IOException {
		HttpServerRequest req = context.request();
		JsonObject errorJson = new JsonObject();

		try {
			if (!LETTER_CHROMOSOMES.contains(chr.toUpperCase())) {
				Integer.parseInt(chr);
			}

			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			errorJson.put(ERROR, INVALID_CHR_OR_POS_ERROR);

			Constants.errorResponse(
				req,
				HttpURLConnection.HTTP_BAD_REQUEST,
				errorJson.toString()
			);

			return;
		}

		byte[] key = UniversalVariantHelper.generateKey(refBuild, chr, Long.parseLong(pos), ref, alt);

		byte[] compressedVariantQueries = dbRep.getBytes(key);
		JsonObject variantQueries = null;

		if (compressedVariantQueries != null) {
			String decompressedVariantQueries = Constants.decompressJson(compressedVariantQueries);
			variantQueries = new JsonObject(decompressedVariantQueries);
		}

		JsonObject result = new JsonObject();

		for (int i = 0; i < FORMAT_NAMES.length; i++) {
			try {
				// Taking class
				Class<?> queryClass = Class.forName("com.astorage.query." + FORMAT_NAMES[i] + "Query");
				// Checking if it overrides normalizedParamsToParams
				Class<?>[] methodParamTypes = new Class<?>[QUERY_PARAMS_COUNT];
				Arrays.fill(methodParamTypes, String.class);
				Method normalizedParamsToParams = queryClass.getMethod("normalizedParamsToParams", methodParamTypes);
				String[] queryParams = (String[]) normalizedParamsToParams.invoke(null, refBuild, chr, pos, ref, alt);

				// If normalizedParamsToParams isn't overridden for a specific query class
				if (queryParams == null) {
					// No stored variant queries for this normalized query
					if (variantQueries == null) {
						continue;
					}

					// WARNING: FORMAT_NAMES constant should NOT be MODIFIED!
					String variantQuery = variantQueries.getString(Integer.toString(i));

					// If no query parameters are stored for this DB format ignore it
					if (variantQuery == null) {
						continue;
					}

					// Store query params
					queryParams = variantQuery.split(QUERY_PARAMS_DELIMITER);
				}

				// Get query results from the specific query class
				Class<?>[] queryParamTypes = new Class<?>[queryParams.length + 1];
				queryParamTypes[0] = RocksDBRepository.class;
				Arrays.fill(queryParamTypes, 1, queryParams.length + 1, String.class);

				RocksDBRepository dbRep = dbRepositories.get(FORMAT_NAMES[i]);

				List<Object> queryParamsWithDBRep = new ArrayList<>();
				queryParamsWithDBRep.add(dbRep);
				queryParamsWithDBRep.addAll(Arrays.asList(queryParams));

				Method queryDataMethod = queryClass.getDeclaredMethod("queryData", queryParamTypes);
				JsonObject queryResult = (JsonObject) queryDataMethod.invoke(null, queryParamsWithDBRep.toArray());

				result.put(FORMAT_NAMES[i], queryResult);
			} catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
                     IllegalAccessException e) {
				throw new RuntimeException(e);
			}
        }

		if (isBatched) {
			req.response().write(result + "\n");
		} else {
			req.response()
				.putHeader("content-type", "text/json")
				.end(result + "\n");
		}
	}
}
