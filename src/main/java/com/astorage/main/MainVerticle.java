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
import io.vertx.core.http.HttpServer;
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
	private final Map<String, RocksDBRepository> dbRepositories = new HashMap<>();

	@Override
	public void start(Promise<Void> startPromise) {
		HttpServer server = vertx.createHttpServer();
		Router router = Router.router(vertx);

		String dataDirectoryPath;
		try {
			dataDirectoryPath = getDataDirectoryPath();
			System.out.println("Data directory path: " + dataDirectoryPath);
		} catch (Exception e) {
			startPromise.fail(e.getMessage());

			return;
		}

		try {
			for (String formatName : FORMAT_NAMES) {
				dbRepositories.put(
					formatName,
					new RocksDBRepository(formatName.toLowerCase(), dataDirectoryPath)
				);
				this.setIngestionHandler(formatName, router);
				this.setQueryHandler(formatName, router);
				this.setBatchQueryHandler(formatName, router);
			}
		} catch (IOException | RocksDBException e) {
			startPromise.fail(ROCKS_DB_INIT_ERROR);

			return;
		}

		setStopHandler(router);

		server.requestHandler(router).listen(8080, result -> {
			if (result.succeeded()) {
				if (!initializeDirectories(dataDirectoryPath)) {
					startPromise.fail(new IOException(INITIALIZING_DIRECTORY_ERROR));

					return;
				}

				System.out.println("HTTP server started on port 8080!");
				startPromise.complete();
			} else {
				System.err.println("Server failed to start...");
				result.cause().printStackTrace();
				startPromise.fail(result.cause());
			}
		});
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		for (RocksDBRepository rocksDBRepository : dbRepositories.values()) {
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

				Ingestor ingestor = (Ingestor) constructor.newInstance(context, dbRepositories.get(formatName));
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

				Query query = (Query) constructor.newInstance(context, dbRepositories.get(formatName));
				query.queryHandler();
			} catch (Exception e) {
				Constants.errorResponse(context.request(), HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
			}
		});
	}

	private void setBatchQueryHandler(String formatName, Router router) {
		router.post("/batch-query/" + formatName.toLowerCase()).handler((RoutingContext context) -> {
			try {
				Class<?> cls = Class.forName("com.astorage.query." + formatName + "BatchQuery");
				Constructor<?> constructor = cls.getConstructor(RoutingContext.class, RocksDBRepository.class);

				Query query = (Query) constructor.newInstance(context, dbRepositories.get(formatName));
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

	private String getDataDirectoryPath() throws Exception {
		List<String> args = Vertx.currentContext().processArgs();
		String dataDirectoryPath = USER_HOME + ASTORAGE_DIRECTORY_NAME;

		if (args != null && args.size() > 0) {
			String configPath = args.get(0);

			File file = new File(configPath);
			if (!file.exists()) {
				throw new Exception(CONFIG_JSON_DOESNT_EXIST_ERROR);
			}

			String configAsString;
			try (FileInputStream fileInputStream = new FileInputStream(file)) {
				byte[] configAsBytes = fileInputStream.readAllBytes();
				configAsString = new String(configAsBytes, StandardCharsets.UTF_8);
			} catch (IOException e) {
				throw new Exception(CONFIG_JSON_NOT_READABLE_ERROR);
			}

			JsonObject configAsJson;
			try {
				configAsJson = new JsonObject(configAsString);
			} catch (DecodeException e) {
				throw new Exception(CONFIG_JSON_DECODE_ERROR);
			}

			String dataDirectoryPathFromConfig = configAsJson.getString(DATA_DIRECTORY_PATH_JSON_KEY);
			if (dataDirectoryPathFromConfig != null) {
				dataDirectoryPath = dataDirectoryPathFromConfig;
			}
		}

		return dataDirectoryPath;
	}
}
