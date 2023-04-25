package com.astorage.main;

import com.astorage.db.RocksDBRepository;
import com.astorage.ingestion.Ingestor;
import com.astorage.query.Query;
import com.astorage.utils.Constants;
import com.astorage.utils.dbnsfp.DbNSFPConstants;
import com.astorage.utils.fasta.FastaConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MainVerticle extends AbstractVerticle implements Constants, FastaConstants, DbNSFPConstants {
	private final Map<String, RocksDBRepository> dbReps = new HashMap<>();

	@Override
	public void start(Promise<Void> startPromise) {
		Router router = Router.router(vertx);
		String dataDirectoryPath = getDataDirectoryPath(startPromise);

		System.out.println(dataDirectoryPath);

		try {
			for (String formatName : FORMAT_NAMES) {
				dbReps.put(formatName, new RocksDBRepository(formatName.toLowerCase(), dataDirectoryPath));
				this.setIngestionHandler(formatName, router);
				this.setQueryHandler(formatName, router);
			}
		} catch (IOException | RocksDBException e) {
			startPromise.fail(ROCKS_DB_INIT_ERROR);
		}

		setStopHandler(router);

		vertx.createHttpServer().requestHandler(router).listen(8080, http -> {
			if (http.succeeded()) {
				if (!initializeDirectories(dataDirectoryPath)) {
					startPromise.fail(new IOException(INITIALIZING_DIRECTORY_ERROR));
				}

				startPromise.complete();
				System.out.println("HTTP server started on port 8080!");
			} else {
				startPromise.fail(http.cause());
			}
		});
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		for (RocksDBRepository rocksDBRepository : dbReps.values()) {
			rocksDBRepository.close();
		}

		System.out.println("HTTP server stopped.");

		stopPromise.complete();
	}

	private void setIngestionHandler(String formatName, Router router) {
		router.post("/ingestion/" + formatName.toLowerCase()).handler((RoutingContext context) -> {
			try {
				Class<?> cls = Class.forName("com.astorage.ingestion." + formatName + "Ingestor");
				Constructor<?> constructor = cls.getConstructor(RoutingContext.class, RocksDBRepository.class);

				Ingestor ingestor = (Ingestor) constructor.newInstance(context, dbReps.get(formatName));
				ingestor.ingestionHandler();
			} catch (Exception e) {
				Constants.errorResponse(context.request(), HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
			}
		});
	}

	private void setQueryHandler(String formatName, Router router) {
		router.get("/query/" + formatName.toLowerCase()).handler((RoutingContext context) -> {
			try {
				Class<?> cls = Class.forName("com.astorage.query." + formatName + "Query");
				Constructor<?> constructor = cls.getConstructor(RoutingContext.class, RocksDBRepository.class);

				Query query = (Query) constructor.newInstance(context, dbReps.get(formatName));
				query.queryHandler();
			} catch (Exception e) {
				Constants.errorResponse(context.request(), HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
			}
		});
	}

	private void setStopHandler(Router router) {
		router.get("/stop").handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();

			req.response()
				.putHeader("content-type", "text/json")
				.end("HTTP server stopped.\n");

			vertx.close();
		});
	}

	private boolean initializeDirectories(String dataDirectoryPath) {
		try {
			File dataDir = new File(dataDirectoryPath);
			if (!dataDir.exists() && !dataDir.mkdirs()) {
				return false;
			}
		} catch (SecurityException e) {
			return false;
		}

		return true;
	}

	private String getDataDirectoryPath(Promise<Void> startPromise) {
		List<String> args = Vertx.currentContext().processArgs();
		String dataDirectoryPath = USER_HOME + ASTORAGE_DIRECTORY_NAME;

		if (args != null && args.size() > 0) {
			String configPath = args.get(0);

			File file = new File(configPath);
			if (!file.exists()) {
				startPromise.fail("Given config file doesn't exist.");
			}

			String configAsString = "";
			try (FileInputStream fileInputStream = new FileInputStream(file)) {
				byte[] configAsBytes = fileInputStream.readAllBytes();
				configAsString = new String(configAsBytes, StandardCharsets.UTF_8);
			} catch (IOException e) {
				startPromise.fail("Couldn't read the given config file...");
			}

			JsonObject configAsJson = null;
			try {
				configAsJson = new JsonObject(configAsString);
			} catch (DecodeException e) {
				startPromise.fail("Given config file isn't a valid JSON.");
			}

			if (configAsJson != null) {
				String dataDirectoryPathFromConfig = configAsJson.getString("data_directory_path");
				if (dataDirectoryPathFromConfig != null) {
					dataDirectoryPath = dataDirectoryPathFromConfig;
				}
			}
		}

		return dataDirectoryPath;
	}
}
