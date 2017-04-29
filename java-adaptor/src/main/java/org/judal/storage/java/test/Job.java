package org.judal.storage.java.test;

import java.sql.Types;

import javax.jdo.JDOException;

import org.judal.metadata.TableDef;
import org.judal.storage.java.ArrayRecord;
import org.judal.storage.table.TableDataSource;


public class Job extends ArrayRecord {

	public static String tableName = "k_jobs";
	
	public static TableDataSource dataSource = null;

	public Job() throws JDOException {
		super(getTableDef(dataSource));
	}

	public static TableDef getTableDef(TableDataSource ds) throws JDOException {
		TableDef tbl;
		if (null==ds)
			tbl = new TableDef(tableName);
		else
			tbl = ds.createTableDef(tableName, null);
		tbl.addPrimaryKeyColumn("default", "gu_job", Types.CHAR, 32);
		tbl.addColumnMetadata("default", "gu_workarea", Types.CHAR, 32, false);
		tbl.addColumnMetadata("default", "gu_writer", Types.CHAR, 32, false);
		tbl.addColumnMetadata("default", "id_command", Types.CHAR, 4, false);
		tbl.addColumnMetadata("default", "dt_created", Types.TIMESTAMP, false);
		tbl.addColumnMetadata("default", "tl_job", Types.VARCHAR, 100, false);
		tbl.addColumnMetadata("default", "gu_job_group", Types.CHAR, 32, false);
		tbl.addColumnMetadata("default", "tx_parameters", Types.VARCHAR, 2000, false);
		tbl.addColumnMetadata("default", "dt_execution", Types.TIMESTAMP, false);
		tbl.addColumnMetadata("default", "dt_finished", Types.TIMESTAMP, false);
		tbl.addColumnMetadata("default", "dt_modified", Types.TIMESTAMP, false);
		tbl.addColumnMetadata("default", "nu_sent", Types.INTEGER, false);
		tbl.addColumnMetadata("default", "nu_opened", Types.INTEGER, false);
		tbl.addColumnMetadata("default", "nu_unique", Types.INTEGER, false);
		tbl.addColumnMetadata("default", "nu_clicks", Types.INTEGER, false);
		return tbl;
	}
	
}
