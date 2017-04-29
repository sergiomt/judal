package org.judal.storage.java.test;

import java.sql.Types;

import javax.jdo.JDOException;

import org.judal.metadata.TableDef;
import org.judal.storage.java.ArrayRecord;
import org.judal.storage.table.TableDataSource;

import com.knowgate.debug.DebugFile;

public class AdhocMailing extends ArrayRecord {

	public static String tableName = "k_adhoc_mailings";
	
	public static TableDataSource dataSource = null;

	public AdhocMailing() throws JDOException {
		super(getTableDef(dataSource));
	}
	
	public static TableDef getTableDef(TableDataSource ds) throws JDOException {
		TableDef tbl;
		if (null==ds)
			tbl = new TableDef(tableName);
		else
			tbl = ds.createTableDef(tableName, null);
		tbl.addPrimaryKeyColumn("default", "gu_mailing", Types.CHAR, 32);
		tbl.addColumnMetadata("default", "gu_workarea", Types.CHAR, 32, false);
		tbl.addColumnMetadata("default", "gu_writer", Types.CHAR, 32, false);
		tbl.addColumnMetadata("default", "pg_mailing", Types.INTEGER, false);
		tbl.addColumnMetadata("default", "dt_created", Types.TIMESTAMP, false);
		tbl.addColumnMetadata("default", "nm_mailing", Types.VARCHAR, 30, false);
		tbl.addColumnMetadata("default", "bo_urgent", Types.SMALLINT, false);
		tbl.addColumnMetadata("default", "bo_reminder", Types.SMALLINT, false);
		tbl.addColumnMetadata("default", "bo_html_part", Types.SMALLINT, false);
		tbl.addColumnMetadata("default", "bo_plain_part", Types.SMALLINT, false);
		tbl.addColumnMetadata("default", "bo_attachments", Types.SMALLINT, false);
		tbl.addColumnMetadata("default", "id_status", Types.VARCHAR, 32, false);
		tbl.addColumnMetadata("default", "dt_modified", Types.TIMESTAMP, false);
		tbl.addColumnMetadata("default", "dt_execution", Types.TIMESTAMP, false);
		tbl.addColumnMetadata("default", "tx_email_from", Types.VARCHAR, 254, false);
		tbl.addColumnMetadata("default", "tx_email_reply", Types.VARCHAR, 254, false);
		tbl.addColumnMetadata("default", "nm_from", Types.VARCHAR, 254, false);
		tbl.addColumnMetadata("default", "tx_subject", Types.VARCHAR, 254, false);
		tbl.addColumnMetadata("default", "tx_allow_regexp", Types.VARCHAR, 254, false);
		tbl.addColumnMetadata("default", "tx_deny_regexp", Types.VARCHAR, 254, false);
		tbl.addColumnMetadata("default", "tx_parameters", Types.VARCHAR, 2000, false);
		return tbl;
	}
	
}
