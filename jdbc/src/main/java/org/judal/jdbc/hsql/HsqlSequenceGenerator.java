package org.judal.jdbc.hsql;

/**
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

/**
 * <p>Sequence for HSQL.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class HsqlSequenceGenerator implements Sequence {

	private JDBCDataSource dataSource;
	private String sequenceName;
	private long current;
	
	public HsqlSequenceGenerator(JDBCDataSource dataSource, String sequenceName) {
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
        try {
			conn = dataSource.getConnection(sequenceName);
			stmt = conn.createStatement();
	        rset = stmt.executeQuery("CALL NEXT VALUE FOR "+sequenceName);
	        rset.next();
	        current = rset.getInt(1);
	        rset.close();
	        rset = null;
	        stmt.close();
	        stmt = null;
	        conn.close(sequenceName);
	        conn = null;
        } catch (SQLException sqle) {
        	throw new JDOException(sqle.getMessage(), sqle);
        } finally {
			try { if (rset!=null) rset.close(); } catch (SQLException ignore) { }
			try { if (stmt!=null) stmt.close(); } catch (SQLException ignore) { }
			try { if (conn!=null) if (!conn.isClosed()) conn.close(sequenceName); } catch (SQLException ignore) { }
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
