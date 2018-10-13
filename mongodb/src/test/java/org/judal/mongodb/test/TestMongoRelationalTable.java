package org.judal.mongodb.test;

import java.io.IOException;

import java.lang.reflect.InvocationTargetException;

import java.sql.Types;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;

import org.judal.metadata.JoinType;
import org.judal.metadata.NameAlias;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;

import org.judal.storage.Param;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Operator;
import org.judal.storage.query.Predicate;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.View;

import org.judal.mongodb.MongoDocument;
import org.judal.mongodb.MongoRelationalDataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import com.knowgate.tuples.Pair;

public class TestMongoRelationalTable extends TestMongoBase {

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
		tdef = new TableDef(collectionName2);
		tdef.addColumnMetadata("", "_id", Types.VARCHAR, false);
		tdef.addColumnMetadata("", "uid", Types.VARCHAR, false);
		tdef.addColumnMetadata("", "trade", Types.FLOAT, false);
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
	public void testView() {
		Map<String, String> properties = new HashMap<>();
		try (MongoRelationalDataSource dts = new MongoRelationalDataSource(properties, null, metadata)) {
			MongoDocument record = new MongoDocument(metadata.getTable(collectionName));
			int count = 0;
			try (View view = dts.openView(record)) {
				Iterator<Stored> iter = view.iterator();
				while (iter.hasNext()) {
					Record r = (Record) iter.next();
					count++;
				}
			}
			assertEquals(doc_count,count);
		}
	}

	@Ignore
	public void testJoinView()
		throws	IOException, JDOUserException, JDOException, UnsupportedOperationException, NoSuchMethodException,
				SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		int count = 0;
		Map<String, String> properties = new HashMap<>();
		try (MongoRelationalDataSource dts = new MongoRelationalDataSource(properties, null, metadata)) {
			TableDef join = metadata.getTable(collectionName).clone();
			join.addColumnMetadata("", "trade", Types.FLOAT, false);
			MongoDocument record = new MongoDocument(join);
			NameAlias days = new NameAlias(collectionName, "d");
			NameAlias trades = new NameAlias(collectionName2, "t");
			try (RelationalView view = dts.openJoinView(JoinType.OUTER, record, days, trades, new Pair("uid","uid"))) {
				assertEquals(3*doc_count,view.count(null, null));
				assertEquals(3,view.count("open", new Float(10)));
				Iterator<Stored> iter = view.iterator();
				String symbol = "";
				float totalTrade = 0f;
				while (iter.hasNext()) {
					Record r = (Record) iter.next();
					symbol = r.getString("symbol");
					assertFalse(r.isNull("trade"));
					count++;
					totalTrade += r.getFloat("trade");
				}
				assertEquals(3*doc_count,count);

				Predicate exc = view.newPredicate().and("exchange", Operator.EQ, "NASDAQ");
				assertNotNull(view.avg("trade", exc));
				assertEquals(totalTrade/(3f*doc_count), view.avg("trade", exc).floatValue(), 0.1);

				RecordSet<MongoDocument> rst = view.fetch(record.fetchGroup(), "symbol", symbol);
				assertEquals(3,rst.size());
				assertEquals(19.25,rst.get(0).getFloat("trade"),0.1);
				assertEquals(19.50,rst.get(1).getFloat("trade"),0.1);
				assertEquals(19.75,rst.get(2).getFloat("trade"),0.1);

				assertEquals(3, view.count("symbol", symbol));
				assertEquals(3, view.count("open", 16.0f));
				assertEquals(1, view.count("trade", 17.25f));

				assertTrue(view.exists(	new Param("open", Types.FLOAT, 16.0f)));
				assertFalse(view.exists(new Param("open", Types.FLOAT, 89.0f)));
				assertTrue(view.exists(	new Param("open", Types.FLOAT, 19.0f),
										new Param("high", Types.FLOAT, 20.0f),
										new Param("trade", Types.FLOAT, 19.25f)));
				assertFalse(view.exists(new Param("open", Types.FLOAT, 19.0f),
										new Param("high", Types.FLOAT, 20.0f),
										new Param("trade", Types.FLOAT, 0.0f)));

				Predicate prd = view.newPredicate().and("symbol", Operator.EQ, symbol).and("open", Operator.GTE, 18).and("trade", Operator.GTE, 19.75);
				assertEquals(1l, view.count(prd).longValue());

				AbstractQuery qry = view.newQuery();
				qry.setFilter(prd);

				assertNotNull(qry.getFilterPredicate());

				rst = view.fetch(qry);
				assertEquals(1, rst.size());

				assertNotNull(qry.getFilterPredicate());

				Record r1 = rst.get(0);
				Record r2 = view.fetchFirst(qry);
				for (String k : r1.asMap().keySet())
					assertEquals(r1.apply(k), r2.apply(k));
			}
		}
	}
}