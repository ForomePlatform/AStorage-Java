package com.astorage.main;

import com.astorage.db.RocksDBRepository;
import com.astorage.ingestion.Ingestor;
import com.astorage.normalization.VariantBatchNormalizer;
import com.astorage.normalization.VariantNormalizer;
import com.astorage.query.Query;
import com.astorage.query.UniversalVariantBatchQuery;
import com.astorage.query.UniversalVariantQuery;
import com.astorage.utils.Constants;
import com.astorage.utils.dbnsfp.DbNSFPConstants;
import com.astorage.utils.fasta.FastaConstants;
import com.astorage.utils.universal_variant.UniversalVariantConstants;
import com.astorage.utils.variant_normalizer.VariantNormalizerConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class MainVerticle extends AbstractVerticle implements Constants, FastaConstants, DbNSFPConstants {
	private final Map<String, RocksDBRepository> dbRepositories = new HashMap<>();
	private final Map<String, WorkerExecutor> workerExecutors = new HashMap<>();
	private String pendingDropRepoRequest = "";

	@Override
	public void start(Promise<Void> startPromise) {
		setSystemProperties();

		HttpServer server = vertx.createHttpServer();
		Router router = Router.router(vertx);

		JsonObject configJson;
		String dataDirectoryPath;
		Integer serverPort = DEFAULT_HTTP_SERVER_PORT;
		try {
			configJson = getConfigJson();

			dataDirectoryPath = getDataDirectoryPath(configJson.getString(DATA_DIRECTORY_PATH_CONFIG_KEY));
			System.out.println("Data directory path: " + dataDirectoryPath);

			createLogFile(dataDirectoryPath);

			if (configJson.containsKey(HTTP_SERVER_PORT_CONFIG_KEY)) {
				serverPort = configJson.getInteger(HTTP_SERVER_PORT_CONFIG_KEY);
			}
		} catch (Exception e) {
			startPromise.fail(e.getMessage());

			return;
		}

		WorkerExecutor initExecutor = vertx.createSharedWorkerExecutor("init-executor", 1, 1, TimeUnit.DAYS);
		Callable<Boolean> callableInit = init(router, startPromise, dataDirectoryPath);

		Integer finalServerPort = serverPort;
		initExecutor.executeBlocking(callableInit).onComplete(handler -> {
			initExecutor.close();

			server.requestHandler(router).listen(finalServerPort, result -> {
				if (result.succeeded()) {
					if (!initializeDirectories(dataDirectoryPath)) {
						startPromise.fail(new IOException(INITIALIZING_DIRECTORY_ERROR));

						return;
					}

					System.out.printf((HTTP_SERVER_START) + "%n", finalServerPort);
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

	private Callable<Boolean> init(Router router, Promise<Void> startPromise, String dataDirectoryPath) {
		return () -> {
			try {
				for (String formatName : FORMAT_NAMES) {
					dbRepositories.put(
							formatName,
							new RocksDBRepository(formatName.toLowerCase(), dataDirectoryPath)
					);

                    createAndStoreFormatWorkerExecutors(formatName);
					setIngestionHandler(formatName, router);
					setQueryHandler(formatName, router);
					setBatchQueryHandler(formatName, router);
				}

				dbRepositories.put(
						UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME,
						new RocksDBRepository(UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME.toLowerCase(), dataDirectoryPath)
				);

                createAndStoreGeneralWorkerExecutors();
				setUniversalVariantQueryHandler(router);
                setUniversalVariantBatchQueryHandler(router);
				setNormalizationHandler(router);
				setBatchNormalizationHandler(router);
			} catch (IOException | RocksDBException e) {
				startPromise.fail(ROCKS_DB_INIT_ERROR);

				return false;
			}

			setDropRepositoryHandler(router);
			setStopHandler(router);
			setSwaggerHandler(router);

			return true;
		};
	}

	/**
	 * For XML...
	 */
	private void setSystemProperties() {
		System.setProperty("entityExpansionLimit", "0");
		System.setProperty("totalEntitySizeLimit", "0");
		System.setProperty("jdk.xml.totalEntitySizeLimit", "0");
	}

	private void createAndStoreFormatWorkerExecutors(String formatName) {
		String ingestionExecutorName = Constants.getIngestionExecutorName(formatName);
		WorkerExecutor ingestionExecutor = vertx.createSharedWorkerExecutor(
			ingestionExecutorName,
			INGESTION_EXECUTOR_POOL_SIZE_LIMIT,
			EXECUTOR_TIME_LIMIT_DAYS,
			TimeUnit.DAYS
		);

		String queryExecutorName = Constants.getQueryExecutorName(formatName);
		WorkerExecutor queryExecutor = vertx.createSharedWorkerExecutor(
			queryExecutorName,
			QUERY_EXECUTOR_POOL_SIZE_LIMIT,
			EXECUTOR_TIME_LIMIT_DAYS,
			TimeUnit.DAYS
		);

		String batchQueryExecutorName = Constants.getBatchQueryExecutorName(formatName);
		WorkerExecutor batchQueryExecutor = vertx.createSharedWorkerExecutor(
			batchQueryExecutorName,
			QUERY_EXECUTOR_POOL_SIZE_LIMIT,
			EXECUTOR_TIME_LIMIT_DAYS,
			TimeUnit.DAYS
		);

		workerExecutors.put(ingestionExecutorName, ingestionExecutor);
		workerExecutors.put(queryExecutorName, queryExecutor);
		workerExecutors.put(batchQueryExecutorName, batchQueryExecutor);
	}

    private void createAndStoreGeneralWorkerExecutors() {
        String universalQueryExecutorName = Constants.getQueryExecutorName(UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME);
        WorkerExecutor univeresalQueryExecutor = vertx.createSharedWorkerExecutor(
                universalQueryExecutorName,
                QUERY_EXECUTOR_POOL_SIZE_LIMIT,
                EXECUTOR_TIME_LIMIT_DAYS,
                TimeUnit.DAYS
        );

        String universalBatchQueryExecutorName = Constants.getBatchQueryExecutorName(UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME);
        WorkerExecutor univeresalBatchQueryExecutor = vertx.createSharedWorkerExecutor(
                universalBatchQueryExecutorName,
                QUERY_EXECUTOR_POOL_SIZE_LIMIT,
                EXECUTOR_TIME_LIMIT_DAYS,
                TimeUnit.DAYS
        );

        String normalizationExecutorName = Constants.getQueryExecutorName(VariantNormalizerConstants.VARIANT_NORMALIZER_FORMAT_NAME);
        WorkerExecutor normalizationQueryExecutor = vertx.createSharedWorkerExecutor(
                normalizationExecutorName,
                QUERY_EXECUTOR_POOL_SIZE_LIMIT,
                EXECUTOR_TIME_LIMIT_DAYS,
                TimeUnit.DAYS
        );

        String batchNormalizationExecutorName = Constants.getBatchQueryExecutorName(VariantNormalizerConstants.VARIANT_NORMALIZER_FORMAT_NAME);
        WorkerExecutor batchNormalizationQueryExecutor = vertx.createSharedWorkerExecutor(
                batchNormalizationExecutorName,
                QUERY_EXECUTOR_POOL_SIZE_LIMIT,
                EXECUTOR_TIME_LIMIT_DAYS,
                TimeUnit.DAYS
        );

        workerExecutors.put(universalQueryExecutorName, univeresalQueryExecutor);
        workerExecutors.put(universalBatchQueryExecutorName, univeresalBatchQueryExecutor);
        workerExecutors.put(normalizationExecutorName, normalizationQueryExecutor);
        workerExecutors.put(batchNormalizationExecutorName, batchNormalizationQueryExecutor);
    }

	private void setIngestionHandler(String formatName, Router router) {
		router.post(INGESTION_URL_PATH + formatName.toLowerCase()).handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();
			String ingestionExecutorName = Constants.getIngestionExecutorName(formatName);
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

			executor.executeBlocking(callable).onComplete(handler -> System.out.println(ingestionExecutorName + " finished working!"));
		});
	}

	private void setQueryHandler(String formatName, Router router) {
		router.get(QUERY_URL_PATH + formatName.toLowerCase()).handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();
			String queryExecutorName = Constants.getQueryExecutorName(formatName);
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

			executor.executeBlocking(callable, false).onComplete(handler -> System.out.println(queryExecutorName + " finished working!"));
		});
	}

	private void setBatchQueryHandler(String formatName, Router router) {
		router.post(BATCH_QUERY_URL_PATH + formatName.toLowerCase()).handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();
			String batchQueryExecutorName = Constants.getBatchQueryExecutorName(formatName);
			WorkerExecutor executor = workerExecutors.get(batchQueryExecutorName);

			Callable<Boolean> callable = () -> {
				System.out.println(batchQueryExecutorName + " started working...");

				try {
					Class<?> cls = Class.forName("com.astorage.query." + formatName + "BatchQuery");
					Constructor<?> constructor = cls.getConstructor(RoutingContext.class, RocksDBRepository.class);

					Query query = (Query) constructor.newInstance(context, dbRepositories.get(formatName));
					query.queryHandler();

					return true;
				} catch (Exception e) {
					Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());

					return false;
				}
			};

			executor.executeBlocking(callable, false).onComplete(handler -> System.out.println(batchQueryExecutorName + " finished working!"));
		});
	}

	private void setUniversalVariantQueryHandler(Router router) {
		router.get(QUERY_URL_PATH + UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME.toLowerCase())
			.handler((RoutingContext context) -> {
				HttpServerRequest req = context.request();
				String queryExecutorName = Constants.getQueryExecutorName(UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME);
				WorkerExecutor executor = workerExecutors.get(queryExecutorName);

                Callable<Boolean> callable = () -> {
                    System.out.println(queryExecutorName + " started working...");

                    try {
                        UniversalVariantQuery universalVariantQuery = new UniversalVariantQuery(
                                context,
                                dbRepositories
                        );

                        universalVariantQuery.queryHandler();

                        return true;
                    } catch (Exception e) {
                        Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());

                        return false;
                    }
                };

                executor.executeBlocking(callable, false).onComplete(handler -> System.out.println(queryExecutorName + " finished working!"));
			});
	}

    private void setUniversalVariantBatchQueryHandler(Router router) {
        router.post(BATCH_QUERY_URL_PATH + UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME.toLowerCase())
                .handler((RoutingContext context) -> {
                    HttpServerRequest req = context.request();
                    String queryExecutorName = Constants.getBatchQueryExecutorName(UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME);
                    WorkerExecutor executor = workerExecutors.get(queryExecutorName);

                    Callable<Boolean> callable = () -> {
                        System.out.println(queryExecutorName + " started working...");

                        try {
                            UniversalVariantBatchQuery universalVariantBatchQuery = new UniversalVariantBatchQuery(
                                    context,
                                    dbRepositories
                            );

                            universalVariantBatchQuery.queryHandler();

                            return true;
                        } catch (Exception e) {
                            Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());

                            return false;
                        }
                    };

                    executor.executeBlocking(callable, false).onComplete(handler -> System.out.println(queryExecutorName + " finished working!"));
                });
    }

	private void setNormalizationHandler(Router router) {
		router.get(NORMALIZATION_URL_PATH).handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();
            String queryExecutorName = Constants.getQueryExecutorName(VariantNormalizerConstants.VARIANT_NORMALIZER_FORMAT_NAME);
            WorkerExecutor executor = workerExecutors.get(queryExecutorName);

            Callable<Boolean> callable = () -> {
                System.out.println(queryExecutorName + " started working...");

                try {
                    VariantNormalizer variantNormalizer = new VariantNormalizer(
                            context,
                            dbRepositories.get(FASTA_FORMAT_NAME)
                    );

                    variantNormalizer.normalizationHandler();

                    return true;
                } catch (Exception e) {
                    Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());

                    return false;
                }
            };

            executor.executeBlocking(callable, false).onComplete(handler -> System.out.println(queryExecutorName + " finished working!"));
		});
	}

	private void setBatchNormalizationHandler(Router router) {
		router.post(BATCH_NORMALIZATION_URL_PATH).handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();
            String queryExecutorName = Constants.getBatchQueryExecutorName(VariantNormalizerConstants.VARIANT_NORMALIZER_FORMAT_NAME);
            WorkerExecutor executor = workerExecutors.get(queryExecutorName);

            Callable<Boolean> callable = () -> {
                System.out.println(queryExecutorName + " started working...");

                try {
                    VariantBatchNormalizer variantBatchNormalizer = new VariantBatchNormalizer(
                            context,
                            dbRepositories.get(FASTA_FORMAT_NAME)
                    );

                    variantBatchNormalizer.normalizationHandler();

                    return true;
                } catch (Exception e) {
                    Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());

                    return false;
                }
            };

            executor.executeBlocking(callable, false).onComplete(handler -> System.out.println(queryExecutorName + " finished working!"));
		});
	}

	private void setDropRepositoryHandler(Router router) {
		router.delete(DROP_REPOSITORY_URL_PATH).handler((RoutingContext context) -> {
			HttpServerRequest req = context.request();

			if ("".equals(pendingDropRepoRequest) && req.params().size() > 1) {
				if (req.params().contains(DROP_REPO_CONFIRM_PARAM)) {
					Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, DROP_REPO_CONFIRM_ON_FIRST_CALL);
				} else {
					Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, DROP_REPO_TOO_MANY_PARAMS);
				}

				return;
			}

			if (req.params().contains(DROP_REPO_FORMAT_NAME_PARAM)) {
				String formatName = req.getParam(DROP_REPO_FORMAT_NAME_PARAM);

				if (!dbRepositories.containsKey(formatName)) {
					pendingDropRepoRequest = "";
					Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, DROP_REPO_NOT_FOUND);
					return;
				} else {
					if (!formatName.equals(pendingDropRepoRequest)) {
						pendingDropRepoRequest = formatName;
						Constants.infoResponse(req, String.format(DROP_REPO_CONFIRM_NOTE, formatName));
						return;
					} else if (DROP_REPO_CONFIRM_VALUE.equals(req.getParam(DROP_REPO_CONFIRM_PARAM))) {
						pendingDropRepoRequest = "";
						RocksDBRepository dbRepository = dbRepositories.get(formatName);

						try {
							if (Arrays.asList(UNIVERSAL_QUERY_FORMAT_NAMES).contains(formatName)) {
								RocksDBRepository universalVariantDbRep = dbRepositories.get(UniversalVariantConstants.UNIVERSAL_VARIANT_FORMAT_NAME);
								universalVariantDbRep.dropColumnFamily(universalVariantDbRep.getColumnFamilyHandle(formatName));
							}
							dbRepository.dropRepository();

							Constants.successResponse(req, String.format(DROP_REPO_SUCCESS, formatName));
							return;
						} catch (RocksDBException e) {
							Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
							return;
						}
					} else {
						pendingDropRepoRequest = "";
						Constants.infoResponse(req, DROP_REPO_NO_CONFIRM);
						return;
					}
				}
			}

			pendingDropRepoRequest = "";
			Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, DROP_REPO_FORMAT_PARAM_MISSING);
		});
	}

	private void setStopHandler(Router router) {
		router.get(STOP_URL_PATH).handler((RoutingContext context) -> {
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

	private String getDataDirectoryPath(String dataDirectoryPathFromConfig) {
		if (dataDirectoryPathFromConfig != null) {
			return dataDirectoryPathFromConfig;
		}

		return USER_HOME + ASTORAGE_DIRECTORY_NAME;
	}

	private void createLogFile(String dataDirectoryPath) throws Exception {
		File logFile = new File(dataDirectoryPath, "output_" + System.currentTimeMillis() + ".log");
		if (!logFile.exists()) {
			Files.createDirectories(logFile.getParentFile().toPath());
			Files.createFile(logFile.getAbsoluteFile().toPath());
		}

		PrintStream printStream = new PrintStream(new FileOutputStream(logFile));
		System.setOut(printStream);
	}

	private JsonObject getConfigJson() throws Exception {
		List<String> args = Vertx.currentContext().processArgs();

		if (args != null && !args.isEmpty()) {
			String configPath = args.get(0);

			return Constants.parseJsonFile(configPath);
		}

		return new JsonObject();
	}
}
