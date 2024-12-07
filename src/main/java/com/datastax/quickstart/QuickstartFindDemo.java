package com.datastax.quickstart;

import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.collections.Collection;
import com.datastax.astra.client.collections.commands.options.CollectionFindOneOptions;
import com.datastax.astra.client.collections.commands.options.CollectionFindOptions;
import com.datastax.astra.client.collections.definition.documents.Document;
import com.datastax.astra.client.core.paging.FindIterable;
import com.datastax.astra.client.core.query.Filter;
import com.datastax.astra.client.core.query.Filters;
import com.datastax.astra.client.core.query.Sort;

import java.util.Optional;

public class QuickstartFindDemo {
 public static void main(String[] args) {
  // Settings
  String token          = System.getenv("ASTRA_DB_APPLICATION_TOKEN");
  String endpoint       = System.getenv("ASTRA_DB_API_ENDPOINT");
  String collectionName = "quickstart_collection";

  // Accessing the (existing) collection
  Collection<Document> collection = new DataAPIClient(token)
   .getDatabase(endpoint)
   .getCollection(collectionName);

  // Perform a vector search to find the closest match
  CollectionFindOneOptions findOneOptions = new CollectionFindOneOptions()
   .sort(Sort.vectorize("A scary novel"));
  collection.findOne(findOneOptions).ifPresent(document -> {
   System.out.println("Here is a scary novel: " + document.getString("title"));
  });

  // Perform a vector search to find the 5 closest matches
  CollectionFindOptions findManyOptions = new CollectionFindOptions()
   .sort(Sort.vectorize("A book set in the arctic"))
   .limit(5);
  System.out.println("Here are some books set in the arctic:");
  collection.find(findManyOptions).forEach(doc -> {
    System.out.println(doc.getString("title"));
  });

  // Find documents that match a filter
  Filter filter = Filters.gt("rating", 4.0);
  CollectionFindOptions options = new CollectionFindOptions().limit(10);
  collection.find(filter, options).forEach(doc -> {
    System.out.println(doc.getString("title") + " is rated " + doc.get("rating"));
  });

 }
}
