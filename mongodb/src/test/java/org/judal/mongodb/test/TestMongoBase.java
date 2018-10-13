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

	protected final String collectionName2 = "test2";

	protected final int doc_count = 10;

	protected MongoClient mongoClient;
	protected MongoDatabase database;
	protected MongoCollection<Document> collection;
	protected MongoCollection<Document> collection2;

	public void setUp() {
		mongoClient = MongoClients.create();
		database = mongoClient.getDatabase(databaseName);
		database.createCollection(collectionName);
		collection = database.getCollection(collectionName);
		collection2 = database.getCollection(collectionName2);
		insertTestData();
	}

	public void tearDown() {
		database.drop();
		mongoClient.close();
	}

	public void insertTestData() {
		final String now = new Date().toString();
		int f = doc_count+1;
		for (int d = 0; d < doc_count; d++) {
			Document doc = new Document();
			String uid = Uid.createUniqueKey();
			doc.append("_id", String.valueOf(d));
			doc.append("uid", uid);
			doc.append("exchange", "NASDAQ");
			doc.append("symbol", Uid.generateRandomId(6, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", Character.UPPERCASE_LETTER));
			doc.append("date", now);
			doc.append("open", new Float(10+d));
			doc.append("high", new Float(11+d));
			doc.append("low", new Float(9+d));
			doc.append("close", new Float(10.3+d));
			collection.insertOne(doc);
			for (int e = 1; e <= 3; e++) {
				Document doc2 = new Document();
				doc2.append("_id", String.valueOf(f++));
				doc2.append("uid", uid);
				Float trade = new Float(10+d+(e*0.25));
				doc2.append("trade", trade);
				collection2.insertOne(doc2);
			}
		}
	}
}
