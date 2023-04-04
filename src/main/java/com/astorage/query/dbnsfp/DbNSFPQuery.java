package com.astorage.query.dbnsfp;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.dbnsfp.DataStorage;
import com.astorage.utils.Constants;
import com.astorage.query.Query;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

import java.net.HttpURLConnection;

public class DbNSFPQuery implements Query, Constants {
	public static final String NUCLEOTIDES = "AGTCU";
	private final RoutingContext context;
	private final RocksDBRepository dbRep;

	public DbNSFPQuery(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	public void queryHandler() {
		HttpServerRequest req = context.request();
		if (!(req.params().size() == 3
			&& req.params().contains("#chr")
			&& req.params().contains("pos(1-based)")
			&& req.params().contains("alt"))) {
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, ERROR_INVALID_PARAMS);
		}

		String chr = req.getParam("#chr");
		String pos = req.getParam("pos(1-based)");
		String alt = req.getParam("alt").toUpperCase();

		try {
			Integer.parseInt(chr);
			Long.parseLong(pos);
		} catch (NumberFormatException e) {
			System.err.println("Invalid <#chr> or <pos(1-based)>...");
			System.exit(1);
		}

		byte[] key = DataStorage.createKey(chr, pos);

		String value = dbRep.find(key);
		if (value == null) {
			System.err.println("Variant doesn't exist for: " + chr + " " + pos);
			System.exit(1);
		}

		if (alt.length() != 1 || !NUCLEOTIDES.contains(alt)) {
			System.err.println("Invalid <alt>...");
			System.exit(1);
		}

		req.response()
			.putHeader("content-type", "text/json")
			.end(value);
	}
}
