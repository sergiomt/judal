package org.judal.mongodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;

import org.judal.mongodb.MongoDocument;
import org.judal.mongodb.MongoTable;
import org.judal.mongodb.MongoTableDataSource;
import org.judal.storage.query.bson.BSONQuery;
import org.judal.storage.table.RecordSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import static org.judal.storage.query.Operator.*;
import static org.judal.storage.query.Connective.*;

public class TestMongoQuery extends TestMongoBase {

	private SchemaMetaData metadata;

	// @Before
	public void setUp() {
		super.setUp();
		metadata = new SchemaMetaData();
		TableDef tdef = new TableDef(collectionName);
		tdef.addColumnMetadata("", "_id", Types.VARCHAR, false);
		tdef.addColumnMetadata("", "uid", Types.VARCHAR, false);
		tdef.addColumnMetadata("", "exchange", Types.VARCHAR, false);
		tdef.addColumnMetadata("", "symbol", Types.VARCHAR, false);
		tdef.addColumnMetadata("", "date", Types.TIMESTAMP, false);
		tdef.addColumnMetadata("", "open", Types.FLOAT, false);
		tdef.addColumnMetadata("", "high", Types.FLOAT, false);
		tdef.addColumnMetadata("", "low", Types.FLOAT, false);
		tdef.addColumnMetadata("", "close", Types.FLOAT, false);
		metadata.addTable(tdef, "default");
	}

	// @After
	public void tearDown() {
		super.tearDown();
	}

	@Test
	public void testVoid() {
	}

	@Ignore
	@SuppressWarnings("unchecked")
	public void testQuery() throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Map<String, String> properties = new HashMap<>();
		try (MongoTableDataSource dts = new MongoTableDataSource(properties, null, metadata)) {
			MongoDocument record = new MongoDocument(metadata.getTable(collectionName));
			try (MongoTable tbl = dts.openTable(record)) {
				
				BSONQuery qry0 = new BSONQuery(tbl);
				qry0.setFilter(qry0.newPredicate(AND).add("exchange", EQ, "NASDAQ").and("open",LT, new Float(0)));
				RecordSet<? extends MongoDocument> rst0 = (RecordSet<? extends MongoDocument>) qry0.execute();
				assertEquals(0, rst0.size());

				BSONQuery qry1 = new BSONQuery(tbl);
				qry1.setFilter(qry1.newPredicate(AND).add("exchange", EQ, "NASDAQ").and("open",GT, new Float(0)));
				RecordSet<? extends MongoDocument> rst1 = (RecordSet<? extends MongoDocument>) qry1.execute();
				assertEquals(doc_count, rst1.size());

				BSONQuery qry2 = new BSONQuery(tbl);
				qry2.setFilter(qry2.newPredicate(AND).add("exchange", EQ, "NASDAQ").and("open",GTE, new Float(14)));
				RecordSet<? extends MongoDocument> rst2 = (RecordSet<? extends MongoDocument>) qry2.execute();
				assertTrue(rst2.size()<rst1.size());

				BSONQuery qry3 = new BSONQuery(tbl);
				qry3.setFilter(qry3.newPredicate(AND).add("exchange", EQ, "NASDAQ").and("open", LT, new Float(14)));
				RecordSet<? extends MongoDocument> rst3 = (RecordSet<? extends MongoDocument>) qry3.execute();
				assertTrue(rst3.size()<rst1.size());

				assertEquals(rst1.size(), rst2.size()+rst3.size());
			}
		}
	}

}
