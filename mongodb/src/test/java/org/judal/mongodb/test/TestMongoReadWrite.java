package org.judal.mongodb.test;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.FieldNameValidator;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecProvider;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.Before;
import org.junit.After;

import com.mongodb.Block;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.client.internal.MongoBatchCursorAdapter;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.Connection;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.FindOperation;

public class TestMongoReadWrite extends TestMongoBase {

	// @Before
	public void setUp() {
		super.setUp();
	}

	// @After
	public void tearDown() {
		super.tearDown();
	}

	@Test
	public void testVoid() {
	}

	@Ignore
	public void testCollection() {

		AtomicInteger count = new AtomicInteger(0);
		long start = System.currentTimeMillis();
		collection.find().forEach((Block<Document>) doc -> count.incrementAndGet());
		long end = System.currentTimeMillis();
		System.out.println(count.get() + " documents read in " + (end - start) + "ms at "
				+ ((1000 * count.get()) / (end - start)) + " docs per sec");
	}

	@Ignore
	public void testCollectionInlined() {

		long start, end;

		ReadPreference readPref = ReadPreference.primary();
		ReadConcern concern = ReadConcern.DEFAULT;
		MongoNamespace ns = new MongoNamespace(databaseName, collectionName);
		Decoder<Document> codec = new DocumentCodec();
		FindOperation<Document> fop = new FindOperation<Document>(ns, codec);
		ReadWriteBinding readBinding = new ClusterBinding(getCluster(), readPref, concern);

		start = System.currentTimeMillis();
		BatchCursor<Document> cursor = fop.execute(readBinding);
		end = System.currentTimeMillis();

		System.out.println("FindOperation execute time " + (end - start) + "ms");
		System.out.println("for BatchCursor " + cursor.getClass().getName());
		// com.mongodb.internal.connection.DefaultServerConnection

		AtomicInteger count = new AtomicInteger(0);
		start = System.currentTimeMillis();
		try (MongoBatchCursorAdapter<Document> cursorAdapter = new MongoBatchCursorAdapter<Document>(cursor)) {
			System.out.println("Cursor adapter class is " + cursorAdapter.getClass().getName());
			while (cursorAdapter.hasNext()) {
				Document doc = cursorAdapter.next();
				count.incrementAndGet();
			}
		}

		end = System.currentTimeMillis();
		System.out.println(count.get() + " inlined documents iterate in " + (end - start) + "ms");
	}

	@Ignore
	public void testConnectionCommand() throws ClassNotFoundException, NoSuchMethodException, SecurityException,
			InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		ReadPreference readPref = ReadPreference.primary();
		ReadConcern concern = ReadConcern.DEFAULT;
		MongoNamespace ns = new MongoNamespace(databaseName, collectionName);
		FieldNameValidator noOpValidator = new NoOpFieldNameValidator();
		DocumentCodec payloadDecoder = new DocumentCodec();
		Constructor<CodecProvider> providerConstructor = (Constructor<CodecProvider>) Class
				.forName("com.mongodb.operation.CommandResultCodecProvider")
				.getDeclaredConstructor(Decoder.class, List.class);
		providerConstructor.setAccessible(true);
		CodecProvider firstBatchProvider = providerConstructor.newInstance(payloadDecoder,
				Collections.singletonList("firstBatch"));
		CodecProvider nextBatchProvider = providerConstructor.newInstance(payloadDecoder,
				Collections.singletonList("nextBatch"));
		Codec<BsonDocument> firstBatchCodec = fromProviders(Collections.singletonList(firstBatchProvider))
				.get(BsonDocument.class);
		Codec<BsonDocument> nextBatchCodec = fromProviders(Collections.singletonList(nextBatchProvider))
				.get(BsonDocument.class);
		ReadWriteBinding readBinding = new ClusterBinding(getCluster(), readPref, concern);
		BsonDocument find = new BsonDocument("find", new BsonString(collectionName));
		Connection conn = readBinding.getReadConnectionSource().getConnection();

		BsonDocument results = conn.command(databaseName, find, noOpValidator, readPref, firstBatchCodec,
				readBinding.getReadConnectionSource().getSessionContext(), true, null, null);
		BsonDocument cursor = results.getDocument("cursor");
		long cursorId = cursor.getInt64("id").longValue();

		BsonArray firstBatch = cursor.getArray("firstBatch");

		System.out.println("first batch size is " + firstBatch.toString());

		/*
		 * for (Entry<String, BsonValue> e : results.entrySet()) {
		 * System.out.println("Got " +
		 * e.getKey()+"="+e.getValue().getClass().getName()); if (e.getValue()
		 * instanceof BsonDocument) { for (Entry<String, BsonValue> f : ((BsonDocument)
		 * e.getValue()).entrySet()) { System.out.println("      " +
		 * f.getKey()+"="+f.getValue().getClass().getName()); } }
		 * 
		 * }
		 */

	}

	private Cluster getCluster() {
		Field cluster, delegate;
		Cluster mongoCluster = null;
		try {
			delegate = mongoClient.getClass().getDeclaredField("delegate");
			delegate.setAccessible(true);
			Object clientDelegate = delegate.get(mongoClient);
			cluster = clientDelegate.getClass().getDeclaredField("cluster");
			cluster.setAccessible(true);
			mongoCluster = (Cluster) cluster.get(clientDelegate);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			System.err.println(e.getClass().getName() + " " + e.getMessage());
		}
		return mongoCluster;
	}

}
