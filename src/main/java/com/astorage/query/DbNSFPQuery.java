package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.dbnsfp.DbNSFPHelper;
import com.astorage.utils.dbnsfp.DbNSFPConstants;
import io.vertx.core.http.HttpServerRequest;
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

		String value = dbRep.find(key);
		if (value == null) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, VARIANT_NOT_FOUND_ERROR);
			return;
		}

		req.response()
			.putHeader("content-type", "text/json")
			.end(value + "\n");
	}
}
