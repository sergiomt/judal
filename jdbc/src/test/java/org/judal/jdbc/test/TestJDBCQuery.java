package org.judal.jdbc.test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;

import java.util.HashMap;
import java.util.Map;
import java.util.AbstractMap.SimpleImmutableEntry;

import javax.jdo.JDOException;
import javax.transaction.SystemException;

import org.judal.jdbc.JDBCEngine;
import org.judal.jdbc.JDBCRelationalView;
import org.judal.metadata.JoinDef;
import org.judal.metadata.TableDef;
import org.judal.storage.java.ArrayRecord;
import org.judal.storage.java.MapRecord;
import org.judal.storage.query.*;
import org.judal.storage.query.sql.SQLAndPredicate;
import org.judal.storage.query.sql.SQLOrPredicate;
import org.judal.storage.query.sql.SQLQuery;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.java.test.Job;
import org.judal.storage.java.test.AdhocMailing;

import org.junit.Test;

import com.knowgate.stringutils.Uid;

import org.junit.Ignore;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestJDBCQuery {

	private static Map<String,String> properties;
	private static TableDataSource dts;

	@BeforeClass
	public static void init() throws ClassNotFoundException, JDOException, IOException {
		properties = new TestJDBC().getTestProperties();
		JDBCEngine jdbc = new JDBCEngine();
		dts = jdbc.getDataSource(properties);
	}

	@AfterClass
	public static void cleanup() throws JDOException {
		if (dts!=null) dts.close();
	}

	@Ignore
	public void test01Join() throws JDOException, IOException, InstantiationException, IllegalAccessException, SystemException {
	    // SELECT SUM(d.nu_msgs),d.dt_execution FROM k_jobs j INNER JOIN k_jobs_atoms_by_day d ON j.gu_job=d.gu_job AND
		// WHERE (j.dt_execution BETWEEN ? AND ? OR j.dt_finished BETWEEN ? AND ?) AND 
        //       j.gu_job IN (SELECT gu_job FROM k_jobs WHERE gu_workarea=?)
        // GROUP BY 2 ORDER BY 2 DESC

		ColumnGroup grp = new ColumnGroup("SUM(d.nu_msgs) AS msg_count","d.dt_execution AS execution_date");
		TableDef jobsTable = dts.createTableDef("k_jobs", new HashMap<String,Object>());
		JoinDef atomsByDay = jobsTable.newJoinMetadata();
		atomsByDay.setColumn("gu_job");
		atomsByDay.setTable("k_jobs_atoms_by_day");
		MapRecord jobAtomByDay = new MapRecord(jobsTable);
	}

	@Test
	public void test01Subselect() throws JDOException, IOException, InstantiationException, IllegalAccessException, SystemException, UnsupportedOperationException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
	
	AdhocMailing.dataSource = dts;

	dts.createTable(AdhocMailing.getTableDef(dts), new HashMap<String,Object>());

	ArrayRecord mailingsRec = new ArrayRecord(dts.getTableDef(AdhocMailing.tableName));
	
	ColumnGroup adhocCols = new ColumnGroup();
    adhocCols.addMembers("gu_mailing","pg_mailing","nm_mailing","tx_subject","0 AS nu_messages","0 AS nu_opened","dt_execution","0 AS nu_clicks");

    Timestamp tsFrom = new Timestamp(System.currentTimeMillis());
    Timestamp tsTo = new Timestamp(System.currentTimeMillis());
    
    Predicate datefilter = new SQLOrPredicate();
    datefilter.add("dt_execution", Operator.BETWEEN, new Timestamp[]{tsFrom,tsTo});
    datefilter.add("dt_finished" , Operator.BETWEEN, new Timestamp[]{tsFrom,tsTo});

    Predicate subselect = new SQLAndPredicate();
    subselect.add("gu_workarea", Operator.EQ, Uid.createUniqueKey());
    subselect.add(datefilter);
    
    Predicate where = new SQLAndPredicate();
    where.add("gu_mailing", Operator.IN, "k_jobs", "gu_job_group", subselect);
	
    IndexableView mailingsTbl = dts.openIndexedView(mailingsRec);
    SQLQuery qry = new SQLQuery(mailingsTbl);
	qry.setResult(adhocCols.getMembers());
	qry.setRange(0, 100);
    qry.setFilter(where);
    qry.setOrdering("dt_execution");
    
    assertEquals("SELECT gu_mailing,pg_mailing,nm_mailing,tx_subject,0 AS nu_messages,0 AS nu_opened,dt_execution,0 AS nu_clicks FROM k_adhoc_mailings WHERE  ( gu_mailing IN (SELECT gu_job_group FROM k_jobs WHERE  ( gu_workarea = ? AND  ( dt_execution BETWEEN ? AND ?  OR dt_finished BETWEEN ? AND ?  )  ) ) )  ORDER BY dt_execution LIMIT 100",qry.source());

    mailingsTbl.close();    
    dts.dropTable(AdhocMailing.tableName, false);

	dts.createTable(Job.getTableDef(dts), new HashMap<String,Object>());
	ArrayRecord jobRec = new ArrayRecord(dts.getTableDef(Job.tableName));

	ColumnGroup jobsByHourCols = new ColumnGroup();
	jobsByHourCols.addMembers("SUM(k_jobs_atoms_by_day.nu_msgs) AS message_count","k_jobs_atoms_by_day.dt_execution AS exec_date");
	
    where = new SQLAndPredicate();
    where.add("gu_job", Operator.IN, "k_jobs", subselect);
	
    JDBCRelationalView jobAtomsJoin = (JDBCRelationalView) dts.openInnerJoinView(jobRec, "k_jobs_atoms_by_day", new SimpleImmutableEntry<String,String>("gu_job","gu_job"));
    
    assertNotNull(jobAtomsJoin.getTableDef());
    assertEquals(Job.tableName, jobAtomsJoin.getTableDef().getName());
    assertEquals(1, jobAtomsJoin.getTableDef().getNumberOfJoins());
    assertEquals(1, jobAtomsJoin.getTableDef().getNumberOfForeignKeys());
    assertEquals("k_jobs_atoms_by_day", jobAtomsJoin.getTableDef().getForeignKeys()[0].getTable());
    assertEquals("k_jobs_atoms_by_day", jobAtomsJoin.getTableDef().getJoins()[0].getTable());
    assertEquals("gu_job", jobAtomsJoin.getTableDef().getForeignKeys()[0].getColumns()[0].getName());
    assertEquals("gu_job", jobAtomsJoin.getTableDef().getJoins()[0].getColumn());

    qry = new SQLQuery(jobAtomsJoin);
	qry.setResult(jobsByHourCols.getMembers());
    qry.setFilter(where);
    qry.setGrouping("2");

    // assertEquals("SELECT SUM(h.nu_msgs) AS message_count,h.dt_hour AS hour FROM k_jobs INNER JOIN k_jobs_atoms_by_day ON k_jobs.gu_job=k_jobs_atoms_by_day.gu_job WHERE  ( gu_job IN (SELECT gu_job FROM k_jobs WHERE  ( gu_workarea = ? AND  ( dt_execution BETWEEN ? AND ?  OR dt_finished BETWEEN ? AND ?  )  ) ) )  GROUP BY 2",qry.source());

    jobAtomsJoin.close();    
    dts.dropTable(Job.tableName, false);

    
    /*
    val atomsbyhour = new DBSubset("k_jobs j INNER JOIN k_jobs_atoms_by_hour h ON j.gu_job=h.gu_job",
			 "SUM(h.nu_msgs),h.dt_hour",
				 "j.dt_execution BETWEEN ? AND ? AND "+
				 "j.gu_job IN (SELECT gu_job FROM k_jobs WHERE gu_workarea=?) GROUP BY 2", 24)
    */

	}
}
