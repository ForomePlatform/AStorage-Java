package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.vep.JsonlParser;
import com.astorage.utils.vep.VEPConstants;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.*;
import java.net.HttpURLConnection;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("unused")
public class VEPIngestor extends Ingestor implements Constants, VEPConstants {
    private String chromosome;

    public VEPIngestor(
            RoutingContext context,
            RocksDBRepository dbRep,
            RocksDBRepository universalVariantDbRep,
            RocksDBRepository fastaDbRep,
            Vertx vertx
    ) {
        super(context, dbRep, universalVariantDbRep, fastaDbRep, vertx);
    }

    public void ingestionHandler() {
        HttpServerRequest req = context.request();

//        if (req.params().size() != 1 || !req.params().contains(DATA_PATH_PARAM)) {
//            Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, INVALID_PARAMS_ERROR);
//
//            return;
//        }
//
//        String dataPath = req.getParam(DATA_PATH_PARAM);
//        File file = new File(dataPath);
//        if (!file.exists()) {
//            Constants.errorResponse(req, HttpURLConnection.HTTP_BAD_REQUEST, FILE_NOT_FOUND_ERROR);
//
//            return;
//        }

        File file = new File("/Users/gshavtvalishvili/Downloads/homo_sapiens_merged_vep_113_GRCh38.tar.gz");
        try (
                FileInputStream fis = new FileInputStream(file);
                GZIPInputStream gis = new GZIPInputStream(fis);
                TarArchiveInputStream tis = new TarArchiveInputStream(gis)) {
            storeData(tis);
        } catch (IOException e) {
            Constants.errorResponse(req, HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
        }

        Constants.successResponse(req, INGESTION_FINISH_MSG);
    }

    private void storeData(TarArchiveInputStream tis) throws IOException {
        TarArchiveEntry entry;
//            while ((entry = tis.getNextEntry()) != null) {
//                if (!entry.isDirectory() && entry.getName().endsWith(".gz")) {
//                    System.out.println("Reading file: " + entry.getName());
//
//                    // Add missing unzip
//                        GZIPInputStream specificFileGis = new GZIPInputStream(tis);
//                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(specificFileGis));
//                    // End unzip
//                    storeData(bufferedReader);
//                }
//            }

        while ((entry = tis.getNextEntry()) != null) {
            if (!entry.isDirectory() && entry.getName().endsWith(".gz")) {
                String outputFilePath = "/Users/gshavtvalishvili/Downloads/temp-cache";
                extractGzFromTarEntry(tis, outputFilePath);
                callDeserializerScript();
                storeExtractedData();
                break;
            }
        }
    }

    public static void extractGzFromTarEntry(TarArchiveInputStream tarInput, String outputFilePath) {
        // Create an output file
        File outputFile = new File(outputFilePath);

        try (FileOutputStream fos = new FileOutputStream(outputFile);
             GZIPInputStream gzipInputStream = new GZIPInputStream(tarInput)) {

            // Buffer for reading
            byte[] buffer = new byte[4096];
            int bytesRead;

            // Read from GZIPInputStream and write to output file
            while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }

            System.out.println("Decompressed and saved file: " + outputFilePath);
//            System.out.println("Saved storable gzip file: " + outputFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void storeExtractedData() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("/Users/gshavtvalishvili/Downloads/temp-output.json"));

        String jsonl;
        int count = 0;
        while ((jsonl = reader.readLine()) != null) {
            if (count != 341) {
                count++;
                continue;
            }
            JsonObject parsedVariation = parseVariation(jsonl);
            System.out.println("Got the JSON Successfully");
            System.out.println(parsedVariation.getJsonObject("_gene"));
            System.out.println(parsedVariation.getJsonObject("translation").containsKey("$id"));
            System.out.println(parsedVariation.getJsonObject("translation").getString("dbID"));
            System.out.println(parsedVariation.getJsonObject("translation").getJsonObject("transcript").getJsonObject("_gene"));

            break;
//            Constants.compressJson(variant.toString());
//            dbRep.saveBytes(key, compressedVariant);
//
//            Constants.logProgress(dbRep, lineCount, 100000);
        }

        reader.close();
    }

    public static void callDeserializerScript() {
        String perlScript = "/Users/gshavtvalishvili/Quantori/AStorage-Java/src/main/java/com/astorage/utils/vep/convert.pl";
        String storableFile = "/Users/gshavtvalishvili/Downloads/temp-cache";
        String jsonFile = "/Users/gshavtvalishvili/Downloads/temp-output.json";

        ProcessBuilder processBuilder = new ProcessBuilder("perl", perlScript, storableFile, jsonFile);
//        processBuilder.directory(new File("path/to/"));

        try {
            Process process = processBuilder.start();

            // Read the output from the command
            BufferedReader processOutputReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line;
            while ((line = processOutputReader.readLine()) != null) {
                System.out.println(line);
            }

            int exitCode = process.waitFor();
            System.out.println("Conversion exited with code: " + exitCode);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private JsonObject parseVariation(String jsonl) {
        JsonParser jsonParser = JsonParser.newParser();
        JsonlParser parser = new JsonlParser();

        return parser.parseJsonlString(jsonl);
    }
}
