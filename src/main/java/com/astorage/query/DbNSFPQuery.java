package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.dbnsfp.DbNSFPConstants;
import com.astorage.utils.dbnsfp.DbNSFPHelper;
import com.astorage.utils.dbnsfp.Variant;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.net.HttpURLConnection;

@SuppressWarnings("unused")
public class DbNSFPQuery implements Query, Constants, DbNSFPConstants {
	private final RoutingContext context;
	private final RocksDBRepository dbRep;

	public DbNSFPQuery(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void queryHandler() {
		HttpServerRequest req = context.request();
		if (!(req.params().size() == 3
			&& req.params().contains(CHR_PARAM)
			&& req.params().contains(POS_PARAM)
			&& req.params().contains(ALT_PARAM))) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);
			return;
		}

		String chr = req.getParam(CHR_PARAM);
		String pos = req.getParam(POS_PARAM);
		String alt = req.getParam(ALT_PARAM).toUpperCase();

		try {
			Integer.parseInt(chr);
			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_CHR_OR_POS_ERROR);
			return;
		}

		if (alt.length() != 1 || !NUCLEOTIDES.contains(alt)) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_ALT_ERROR);
			return;
		}

		byte[] key = DbNSFPHelper.createKey(chr, pos);

		String variantsString = dbRep.find(key);
		if (variantsString == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, VARIANT_NOT_FOUND_ERROR);
			return;
		}

		JsonObject result = new JsonObject();
		result.put(CHR_PARAM, chr);
		result.put(POS_PARAM, pos);
		result.put(ALT_PARAM, alt);

		JsonArray finalVariantsJson = new JsonArray();
		JsonArray variantsJson = new JsonArray(variantsString);

		for (int i = 0; i < variantsJson.size(); i++) {
			JsonObject variantJson = variantsJson.getJsonObject(i);
			String nucleotide = variantJson.getString(Variant.VARIANT_ALT);

			if (nucleotide.equals(alt)) {
				finalVariantsJson.add(variantJson);
			}
		}

		result.put("variants", finalVariantsJson);

		req.response()
			.putHeader("content-type", "text/json")
			.end(result + "\n");
	}
}
