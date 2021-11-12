package org.judal.jdbc.postgresql;

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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.jdo.JDOException;
import javax.jdo.datastore.Sequence;

import org.judal.jdbc.JDBCDataSource;
import org.judal.jdbc.jdc.JDCConnection;

import com.knowgate.debug.DebugFile;

public class PgSequenceGenerator implements Sequence {

	private JDBCDataSource dataSource;
	private String sequenceName;
	private long current;

	public PgSequenceGenerator(JDBCDataSource dataSource, String sequenceName) {
		this.dataSource = dataSource;
		this.sequenceName = sequenceName;
	}

	@Override
	public String getName() {
		return sequenceName;
	}

	@Override
	public long nextValue() throws JDOException {
		JDCConnection conn = null;
		Statement stmt = null;
		ResultSet rset = null;
		if (DebugFile.trace) {
			DebugFile.writeln("Begin PgSequenceGenerator.nextValue()");
			DebugFile.incIdent();
		}
		try {
			conn = dataSource.getConnection(sequenceName);
			stmt = conn.createStatement();
			if (DebugFile.trace)
				DebugFile.writeln("Statement.executeQuery(SELECT nextval('" + getName() + "'))");
			rset = stmt.executeQuery("SELECT nextval('" + getName() + "')");
			rset.next();
			current = rset.getInt(1);
			rset.close();
			rset = null;
			stmt.close();
			stmt = null;
			conn.close(sequenceName);
			conn = null;
		} catch (SQLException sqle) {
			if (DebugFile.trace) {
				DebugFile.writeln(sqle.getClass().getName() + " " + sqle.getMessage());
				DebugFile.decIdent();
			}
			throw new JDOException(sqle.getMessage(), sqle);
		} finally {
			try {
				if (rset != null)
					rset.close();
			} catch (SQLException ignore) {
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException ignore) {
			}
			try {
				if (conn != null)
					if (!conn.isClosed())
						conn.close(sequenceName);
			} catch (SQLException ignore) {
			}
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End PgSequenceGenerator.nextValue() : " + current);
		}
		return current;
	}

	@Override
	public void allocate(int arg0) {
	}

	@Override
	public Object current() {
		return new Long(current);
	}

	@Override
	public long currentValue() {
		return current;
	}

	@Override
	public Object next() {
		return new Long(nextValue());
	}

}
