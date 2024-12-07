package com.datastax.quickstart;

import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.collections.Collection;
import com.datastax.astra.client.collections.definition.CollectionDefinition;
import com.datastax.astra.client.collections.definition.documents.Document;
import com.datastax.astra.client.databases.Database;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QuickstartUploadDemo {

 public static void main(String[] args) {
  // Settings
  String token          = System.getenv("ASTRA_DB_APPLICATION_TOKEN");
  String endpoint       = System.getenv("ASTRA_DB_API_ENDPOINT");
  String jsonFile       = "src/main/resources/quickstart_dataset.json";
  String collectionName = "quickstart_collection";

  // Load JSON Dataset
  List<Document> docs = loadJson(jsonFile);
  System.out.println("Loaded " + docs.size() + " documents from " + jsonFile);

  // For each document add a $vectorize field
  docs.forEach(doc -> {
    String summary = doc.getString("summary");
    String genres  = String.join(", ", doc.getList("genres", String.class));
    doc.vectorize("summary: " + summary
            + " | genres: " + genres);
  });

  // Connect to Database
  Database database = new DataAPIClient(token).getDatabase(endpoint);

  // Create Collection for your documents
  Collection<Document> collection = database.createCollection(
    collectionName, new CollectionDefinition()
     .vectorize("nvidia", "NV-Embed-QA"));

  collection.deleteAll();
  // Upload documents to the collection
  collection.insertMany(docs);
  System.out.println("Uploaded "
          + collection.countDocuments(1000)
          + " documents to "
          + collectionName);
 }

/**
 * Loads any JSON file into a list of Document objects.
 *
 * @param filename
 *      the name of the file to load
 * @return
 *      a list of Document objects
 */
 public static List<Document> loadJson(String filename) {
   List<Document> documentList = new ArrayList<>();
   JsonFactory factory = new JsonFactory();
   ObjectMapper mapper = new ObjectMapper();
   try (JsonParser parser = factory.createParser(new File(filename))) {
     if (parser.nextToken() == JsonToken.START_ARRAY) {
       while (parser.nextToken() == JsonToken.START_OBJECT) {
         documentList.add(mapper.readValue(parser, Document.class));
       }
     }
     return documentList;
   } catch (IOException e) {
     throw new RuntimeException("Cannot parse Json file");
   }
 }

}
