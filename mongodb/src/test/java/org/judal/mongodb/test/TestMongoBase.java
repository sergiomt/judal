package org.judal.mongodb.test;

import java.util.Date;

import org.bson.Document;

import com.knowgate.stringutils.Uid;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class TestMongoBase {

	protected final String databaseName = "localhost";

	protected final String collectionName = "test";

	protected final int doc_count = 10;

	protected MongoClient mongoClient;
	protected MongoDatabase database;
	protected MongoCollection<Document> collection;

	public void setUp() {
		mongoClient = MongoClients.create();
		database = mongoClient.getDatabase(databaseName);
		database.createCollection(collectionName);
		collection = database.getCollection(collectionName);
		insertTestData();
	}

	public void tearDown() {
		database.drop();
		mongoClient.close();
	}

	public void insertTestData() {
		final String now = new Date().toString();
		for (int d = 0; d < doc_count; d++) {
			Document doc = new Document();
			doc.append("_id", String.valueOf(d));
			doc.append("uid", Uid.createUniqueKey());
			doc.append("exchange", "NASDAQ");
			doc.append("symbol", Uid.generateRandomId(6, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", Character.UPPERCASE_LETTER));
			doc.append("date", now);
			doc.append("open", new Float(10+d));
			doc.append("high", new Float(11+d));
			doc.append("low", new Float(9+d));
			doc.append("close", new Float(10.3+d));
			collection.insertOne(doc);
		}
	}

}
