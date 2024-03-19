package com.astorage.main;

import com.astorage.db.RocksDBRepository;
import com.astorage.ingestion.Ingestor;
import com.astorage.normalization.VariantBatchNormalizer;
import com.astorage.normalization.VariantNormalizer;
import com.astorage.query.Query;
import com.astorage.query.UniversalVariantQuery;
import com.astorage.utils.Constants;
import com.astorage.utils.dbnsfp.DbNSFPConstants;
import com.astorage.utils.fasta.FastaConstants;
import com.astorage.utils.universal_variant.UniversalVariantConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import org.rocksdb.RocksDBException;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class MainVerticle extends AbstractVerticle implements Constants, FastaConstants, DbNSFPConstants {
	private final Map<String, RocksDBRepository> dbRepositories = new HashMap<>();
	private final Map<String, WorkerExecutor> workerExecutors = new HashMap<>();

	@Override
	public void start(Promise<Void> startPromise) {
		setSystemProperties();

		HttpServer server = vertx.createHttpServer();
		Router router = Router.router(vertx);

		String dataDirectoryPath;
		try {
			dataDirectoryPath = getDataDirectoryPath();
			System.out.println("Data directory path: " + dataDirectoryPath);

			File logFile = new File(dataDirectoryPath, "output.log");
			if (!logFile.exists()) {
				Files.createDirectories(logFile.getParentFile().toPath());
				Files.createDirectory(logFile.getAbsoluteFile().toPath());
			}

			PrintStream printStream = new PrintStream(new FileOutputStream(logFile));
			System.setOut(printStream);
		} catch (Exception e) {
			startPromise.fail(e.getMessage());

			return;
		}

		WorkerExecutor initExecutor = vertx.createSharedWorkerExecutor("init-executor", 1, 1, TimeUnit.DAYS);

		Callable<Boolean> callable = () -> {
			try {
				dbRepositories.put(
					UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME,
					new RocksDBRepository(UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME.toLowerCase(), dataDirectoryPath)
				);

				this.setUniversalVariantQueryHandler(router);

				for (String formatName : FORMAT_NAMES) {
					dbRepositories.put(
						formatName,
						new RocksDBRepository(formatName.toLowerCase(), dataDirectoryPath)
					);

					this.createAndStoreWorkerExecutors(formatName);

					this.setIngestionHandler(formatName, router);
					this.setQueryHandler(formatName, router);
					this.setBatchQueryHandler(formatName, router);
				}

				this.setNormalizationHandler(router);
				this.setBatchNormalizationHandler(router);
			} catch (IOException | RocksDBException e) {
				startPromise.fail(ROCKS_DB_INIT_ERROR);

				return false;
			}

			setStopHandler(router);
			this.setSwaggerHandler(router);

			return true;
		};

		initExecutor.executeBlocking(callable).onComplete(handler -> {
			initExecutor.close();

			server.requestHandler(router).listen(HTTP_SERVER_PORT, result -> {
				if (result.succeeded()) {
					if (!initializeDirectories(dataDirectoryPath)) {
						startPromise.fail(new IOException(INITIALIZING_DIRECTORY_ERROR));

						return;
					}

					System.out.println(HTTP_SERVER_START);
					startPromise.complete();
				} else {
					System.err.println(HTTP_SERVER_FAIL);
					result.cause().printStackTrace();
					startPromise.fail(result.cause());
				}
			});
		});
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		for (WorkerExecutor workerExecutor : workerExecutors.values()) {
			workerExecutor.close();
		}

		for (RocksDBRepository rocksDBRepository : dbRepositories.values()) {
			rocksDBRepository.close();
		}

		System.out.println(HTTP_SERVER_STOP);

		stopPromise.complete();
	}

	/**
	 * For XML...
	 */
	private void setSystemProperties() {
		System.setProperty("entityExpansionLimit", "0");
		System.setProperty("totalEntitySizeLimit", "0");
		System.setProperty("jdk.xml.totalEntitySizeLimit", "0");
	}

	private void createAndStoreWorkerExecutors(String formatName) {
		String ingestionExecutorName = formatName + INGESTION_EXECUTOR_SUFFIX;
		WorkerExecutor ingestionExecutor = vertx.createSharedWorkerExecutor(
			ingestionExecutorName,
			INGESTION_EXECUTOR_POOL_SIZE_LIMIT,
			EXECUTOR_TIME_LIMIT_DAYS,
			TimeUnit.DAYS
		);

		String queryExecutorName = formatName + QUERY_EXECUTOR_SUFFIX;
		WorkerExecutor queryExecutor = vertx.createSharedWorkerExecutor(
			formatName + QUERY_EXECUTOR_SUFFIX,
			QUERY_EXECUTOR_POOL_SIZE_LIMIT,
			EXECUTOR_TIME_LIMIT_DAYS,
			TimeUnit.DAYS
		);

		workerExecutors.put(ingestionExecutorName, ingestionExecutor);
		workerExecutors.put(queryExecutorName, queryExecutor);
	}

	private void setIngestionHandler(String formatName, Router router) {
		router.post("/ingestion/" + formatName.toLowerCase()).handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();
			String ingestionExecutorName = formatName + INGESTION_EXECUTOR_SUFFIX;
			WorkerExecutor executor = workerExecutors.get(ingestionExecutorName);

			Callable<Boolean> callable = () -> {
				System.out.println(ingestionExecutorName + " started working...");

				try {
					Class<?> cls = Class.forName("com.astorage.ingestion." + formatName + "Ingestor");
					Constructor<?> constructor = cls.getConstructor(
						RoutingContext.class,
						RocksDBRepository.class,
						RocksDBRepository.class,
						RocksDBRepository.class
					);

					Ingestor ingestor = (Ingestor) constructor.newInstance(
						context,
						dbRepositories.get(formatName),
						dbRepositories.get(UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME),
						dbRepositories.get(FastaConstants.FASTA_FORMAT_NAME)
					);
					ingestor.ingestionHandler();

					return true;
				} catch (Exception e) {
					Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());

					return false;
				}
			};

			executor.executeBlocking(callable).onComplete(handler -> {
				System.out.println(ingestionExecutorName + " finished working!");
			});
		});
	}

	private void setQueryHandler(String formatName, Router router) {
		router.get("/query/" + formatName.toLowerCase()).handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();
			String queryExecutorName = formatName + QUERY_EXECUTOR_SUFFIX;
			WorkerExecutor executor = workerExecutors.get(queryExecutorName);

			Callable<Boolean> callable = () -> {
				System.out.println(queryExecutorName + " started working...");

				try {
					Class<?> cls = Class.forName("com.astorage.query." + formatName + "Query");
					Constructor<?> constructor = cls.getConstructor(RoutingContext.class, RocksDBRepository.class);

					Query query = (Query) constructor.newInstance(context, dbRepositories.get(formatName));
					query.queryHandler();

					return true;
				} catch (Exception e) {
					Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());

					return false;
				}
			};

			executor.executeBlocking(callable, false).onComplete(handler -> {
				System.out.println(queryExecutorName + " finished working!");
			});
		});
	}

	private void setBatchQueryHandler(String formatName, Router router) {
		router.post("/batch-query/" + formatName.toLowerCase()).handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();

			try {
				Class<?> cls = Class.forName("com.astorage.query." + formatName + "BatchQuery");
				Constructor<?> constructor = cls.getConstructor(RoutingContext.class, RocksDBRepository.class);

				Query query = (Query) constructor.newInstance(context, dbRepositories.get(formatName));
				query.queryHandler();
			} catch (Exception e) {
				Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
			}
		});
	}

	private void setUniversalVariantQueryHandler(Router router) {
		router.get("/query/" + UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME.toLowerCase())
			.handler((RoutingContext context) -> {
				HttpServerRequest req = context.request();

				try {
					UniversalVariantQuery universalVariantQuery = new UniversalVariantQuery(
						context,
						dbRepositories
					);

					universalVariantQuery.queryHandler();
				} catch (Exception e) {
					Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
				}
			});
	}

	private void setNormalizationHandler(Router router) {
		router.get("/normalization").handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();

			try {
				VariantNormalizer variantNormalizer = new VariantNormalizer(
					context,
					dbRepositories.get(FASTA_FORMAT_NAME)
				);

				variantNormalizer.normalizationHandler();
			} catch (Exception e) {
				Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
			}
		});
	}

	private void setBatchNormalizationHandler(Router router) {
		router.post("/batch-normalization").handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();

			try {
				VariantBatchNormalizer variantBatchNormalizer = new VariantBatchNormalizer(
					context,
					dbRepositories.get(FASTA_FORMAT_NAME)
				);

				variantBatchNormalizer.normalizationHandler();
			} catch (Exception e) {
				Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
			}
		});
	}

	private void setStopHandler(Router router) {
		router.get("/stop").handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();

			Constants.successResponse(req, HTTP_SERVER_STOP);

			vertx.close();
		});
	}

	private void setSwaggerHandler(Router router) {
		router.route("/*").handler(StaticHandler.create());
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

		if (args != null && !args.isEmpty()) {
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
