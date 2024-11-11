package com.astorage.utils.vep;

import com.astorage.utils.Pair;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonlParser {
    private final Map<String, JsonObject> idMap = new HashMap<>();

    public JsonObject parseJsonlString(String line) {
        if (line.isBlank()) return null;

        JsonObject jsonObject = new JsonObject(line);

        // Resolve all $ref references
        replaceRefs(jsonObject);

        return jsonObject;
    }

    private void processObject(JsonObject jsonObject) {
        if (jsonObject.containsKey("$id")) {
            // Store object with $id for later reference
            String id = jsonObject.getString("$id");
            idMap.put(id, jsonObject);
            jsonObject.remove("$id");
        }
    }

    private void replaceRefs(JsonObject jsonObject) {
        processObject(jsonObject);
        List<Pair<String, JsonObject>> itemsToReplace = new ArrayList<>();

        jsonObject.forEach(entry -> {
            Object value = entry.getValue();

            if (value instanceof JsonObject nestedObject) {

                if (nestedObject.containsKey("$ref")) {
                    String refId = nestedObject.getString("$ref");
                    JsonObject referencedObject = idMap.get(refId);

                    if (referencedObject != null) {
                        itemsToReplace.add(new Pair<>(entry.getKey(), referencedObject));
                    }
                } else {
                    replaceRefs(nestedObject);
                }
            } else if (value instanceof JsonArray array) {
                for (int j = 0; j < array.size(); j++) {
                    if (array.getValue(j) instanceof JsonObject) {
                        replaceRefs(array.getJsonObject(j));
                    }
                }
            }
        });

        itemsToReplace.forEach(item -> {
            jsonObject.put(item.key(), item.value());
        });
    }
}