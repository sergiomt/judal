package org.judal.mongodb.test;

import org.junit.Test;


import org.junit.Ignore;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertArrayEquals;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.mongodb.MongoDocument;
import org.judal.mongodb.MongoTable;
import org.judal.mongodb.MongoTableDataSource;
import org.judal.storage.Param;
import org.judal.storage.table.RecordSet;

public class TestMongoTable extends TestMongoBase {

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
	public void testTable() {
		Map<String, String> properties = new HashMap<>();
		try (MongoTableDataSource dts = new MongoTableDataSource(properties, null, metadata)) {
			assertTrue(dts.exists(collectionName, "U"));
			TableDef tdef = metadata.getTable(collectionName);
			assertNotNull(tdef);
			MongoDocument record = new MongoDocument(tdef);
			assertEquals(collectionName, record.getTableName());
			try (MongoTable tbl = dts.openTable(record)) {
				assertArrayEquals(tdef.getColumns(), tbl.columns());
				RecordSet<MongoDocument> rset = tbl.fetch(record.fetchGroup(), "exchange", "NASDAQ");
				assertEquals(doc_count, rset.size());
				rset = tbl.fetch(record.fetchGroup(), "exchange", "IBEX");
				assertEquals(0, rset.size());
				rset = tbl.fetch(record.fetchGroup(), 5, 0, new Param[] {new Param("exchange", Types.VARCHAR, 1, "NASDAQ")});
				assertEquals(5, rset.size());
				rset = tbl.fetch(record.fetchGroup(), 5, doc_count-2, new Param[] {new Param("exchange", Types.VARCHAR, 1, "NASDAQ")});
				assertEquals(2, rset.size());
				rset = tbl.fetch(record.fetchGroup(), "open", new Float(10), new Float(14));
				assertEquals(5, rset.size());
				rset = tbl.fetch(record.fetchGroup(), "open", new Float(10), new Float(14), 2, 0);
				assertEquals(2, rset.size());
				assertEquals(1, tbl.count("open", new Float(10)));
			}
		}
	}
}
