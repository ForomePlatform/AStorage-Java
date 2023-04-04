package com.astorage.main;

import com.astorage.db.RocksDBRepository;
import com.astorage.ingestion.Ingestor;
import com.astorage.query.Query;
import com.astorage.utils.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

public class MainVerticle extends AbstractVerticle implements Constants, com.astorage.utils.fasta.Constants, com.astorage.utils.dbnsfp.Constants {
	private Map<String, RocksDBRepository> dbReps;

	@Override
	public void start(Promise<Void> startPromise) {
		Router router = Router.router(vertx);
		dbReps = new HashMap<>();

		for (String formatName : FORMAT_NAMES) {
			dbReps.put(formatName, new RocksDBRepository(formatName.toLowerCase()));
			this.setIngestionHandler(formatName, router);
			this.setQueryHandler(formatName, router);
		}

		vertx.createHttpServer().requestHandler(router).listen(8080, http -> {
			if (http.succeeded()) {
				startPromise.complete();
				System.out.println("HTTP server started on port 8080");
				if (!initializeDirectories()) {
					startPromise.fail(new IOException(ERROR_INITIALIZING_DIRECTORY));
				}
			} else {
				startPromise.fail(http.cause());
			}
		});
	}

	private void setIngestionHandler(String formatName, Router router) {
		router.post("/ingestion/" + formatName.toLowerCase() + "/").handler(
			(RoutingContext context) -> {
				try {
					Class<?> cls = Class.forName("com.astorage.ingestion." + formatName + "Ingestor");
					((Ingestor) cls.getDeclaredConstructor().newInstance(context, dbReps.get(formatName))).ingestionHandler();
				} catch (Exception e) {
					Constants.errorResponse(context.request(), HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
				}
			}
		);
	}

	private void setQueryHandler(String formatName, Router router) {
		router.get("/query/" + formatName.toLowerCase() + "/").handler((RoutingContext context) -> {
			try {
				Class<?> cls = Class.forName("com.astorage.query." + formatName + "Query");
				((Query) cls.getDeclaredConstructor().newInstance(context, dbReps.get(formatName))).queryHandler();
			} catch (Exception e) {
				Constants.errorResponse(context.request(), HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
			}
		});
	}

	private boolean initializeDirectories() {
		try {
			File dataDir = new File(DATA_DIRECTORY_PATH);
			if (!dataDir.exists() && !dataDir.mkdirs()) {
				return false;
			}
		} catch(SecurityException e) {
			return false;
		}

		return true;
	}
}
