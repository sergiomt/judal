package org.judal.bdb;

/*
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.util.Properties;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;
import org.judal.storage.ForeignKeyChecker;
import org.judal.storage.Param;

import com.knowgate.debug.DebugFile;

public class DBForeignKeyChecker implements ForeignKeyChecker {

	private DBTableDataSource dataSource;
	
	public DBForeignKeyChecker(DBTableDataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	@Override
	public boolean exists(String tableName, String columnName, Object columnValue) throws JDOException {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin DBForeignKeyChecker.exists("+tableName+", "+columnName+", "+columnValue+")");
			DebugFile.incIdent();
		}

		boolean retval;
		DBTable target = null;
		try {
			Properties oProps = new Properties();
			oProps.put("name", tableName);
			oProps.put("readonly", "true");
			target = (DBTable) dataSource.openTableOrBucket(oProps, dataSource.getTableDef(tableName), dataSource.getMetaData().getTable(tableName).getRecordClass(), true);
			retval = target.exists(new Param(columnName, ColumnDef.typeForObject(columnValue), 1, columnValue));
		} catch (IllegalArgumentException | IllegalStateException | ClassNotFoundException e) {
			throw new JDOException(e.getMessage(), e);
		} finally {
			target.close();
		}

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End DBForeignKeyChecker.exists() : " +  String.valueOf(retval));
		}
		
		return retval;
	}
	
}
