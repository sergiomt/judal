package org.judal.jdbc.metadata;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;

import org.judal.jdbc.JDBCMetadataObjectFactory;
import org.judal.jdbc.RDBMS;
import org.judal.metadata.ExtendableDef;
import org.judal.metadata.MetadataScanner;
import org.judal.metadata.ProcedureDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.Scriptable;
import org.judal.metadata.TriggerDef;
import org.judal.metadata.ViewDef;

import com.knowgate.io.StreamPipe;

/**
 * <p>Read SQL object definition from an InputStream.</p>
 * The InputStream must contain only one object which may be SEQUENCE, VIEW, PROCEDURE, FUNCTION or TRIGGER
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class SQLDdlMetadata implements MetadataScanner {

	private RDBMS dbms;
	private String statementDelimiter;

	public SQLDdlMetadata(RDBMS dbms) {
		this.dbms = dbms;
		this.statementDelimiter = ";\n";
	}

	/**
	 * Get SQL-DDL statement delimiter. By default it is ; followed by a new line character LF(10)
	 * @return String
	 */
	public String getstatementDelimiter() {
		return statementDelimiter;
	}

	public void setstatementDelimiter(final String delim) {
		statementDelimiter = delim;
	}

	private String readStream(InputStream instrm) throws IOException {
		StreamPipe pipe = new StreamPipe();
		ByteArrayOutputStream raw = new ByteArrayOutputStream(8000);
		pipe.between(instrm, raw);
		String contents = raw.toString("UTF8");
		raw.close();
		return contents;
	}

	/**
	 * <p>Read SQL object definition from an InputStream.</p>
	 * @param instrm InputStream Stream contents must be UTF-8 encoded.
	 * @return SchemaMetaData
	 */
	@Override
	public SchemaMetaData readMetadata(InputStream instrm) throws JDOUserException, JDOException, IOException {
		try {
			return readMetadata(readStream(instrm));
		} catch (NoSuchMethodException nsme) {
			throw new JDOException(nsme.getMessage());
		}
	}

	/**
	 * <p>Read SQL object definition from a String.</p>
	 * @param sql String SEQUENCE, VIEW, PROCEDURE, FUNCTION or TRIGGER definition
	 * @return SchemaMetaData
	 * @throws JDOUserException
	 * @throws JDOException
	 * @throws IOException
	 * @throws NoSuchMethodException
	 */
	public SchemaMetaData readMetadata(String sql) throws JDOUserException, JDOException, IOException, NoSuchMethodException {
		Matcher match;
		SchemaMetaData metadata = new SchemaMetaData();
		if (sql.trim().length()>0) {
			Pattern seq = Pattern.compile("CREATE +SEQUENCE +(\\S+)", Pattern.CASE_INSENSITIVE);
			Pattern view = Pattern.compile("CREATE +(OR +REPLACE +)?VIEW +(\\S+)", Pattern.CASE_INSENSITIVE);
			Pattern proc = Pattern.compile("CREATE +(OR +REPLACE +)?PROCEDURE +(\\S+)", Pattern.CASE_INSENSITIVE);
			Pattern func = Pattern.compile("CREATE +(OR +REPLACE +)?FUNCTION +(\\S+)", Pattern.CASE_INSENSITIVE);
			Pattern trig = Pattern.compile("CREATE +(OR +REPLACE +)?TRIGGER +(\\S+)", Pattern.CASE_INSENSITIVE);
			int seqCount = 0, viewCount = 0, procCount = 0, funcCount = 0, trigCount = 0;
			match = seq.matcher(sql);
			while (match.find()) seqCount++;
			match = view.matcher(sql);
			while (match.find()) viewCount++;
			match = proc.matcher(sql);
			while (match.find()) procCount++;
			match = func.matcher(sql);
			while (match.find()) funcCount++;
			match = trig.matcher(sql);
			while (match.find()) trigCount++;
			if (seqCount==0 && viewCount==0 && procCount==0 && funcCount==0 && trigCount==0)
				throw new JDOUserException("Cannot find a valid SEQUENCE, VIEW, PROCEDURE, FUNCTION or TRIGGER");
			if (seqCount>1)
				throw new JDOUserException("Not more than one SEQUENCE can be defined on each file");
			if (viewCount>1)
				throw new JDOUserException("Not more than one VIEW can be defined on each file");
			if (procCount>1)
				throw new JDOUserException("Not more than one PROCEDURE can be defined on each file");
			if (funcCount>1)
				throw new JDOUserException("Not more than one FUNCTION can be defined on each file");
			if (trigCount>1)
				throw new JDOUserException("Not more than one TRIGGER can be defined on each file");
			if (viewCount+procCount+funcCount+trigCount>1)
				throw new JDOUserException("Not more than one VIEW, PROCEDURE, FUNCTION or TRIGGER can be defined on each file");

			if (viewCount==1) {
				match = view.matcher(sql);
				match.find();
				ViewDef viewDef = JDBCMetadataObjectFactory.newViewDef(dbms, match.group(1), sql);
				metadata.addView(viewDef);
			}

			if (procCount==1) {
				match = proc.matcher(sql);
				match.find();
				ProcedureDef procDef = JDBCMetadataObjectFactory.newProcedureDef(dbms);
				procDef.setName(match.group(1));
				procDef.setSource(match.group(0));
				metadata.addProcedure(procDef);
			}

			if (funcCount==1) {
				match = func.matcher(sql);
				match.find();
				ProcedureDef funcDef = JDBCMetadataObjectFactory.newProcedureDef(dbms);
				funcDef.setName(match.group(1));
				funcDef.setSource(match.group(0));
				metadata.addProcedure(funcDef);
			}

			if (trigCount==1) {
				match = func.matcher(sql);
				match.find();
				TriggerDef trigDef = JDBCMetadataObjectFactory.newTriggerDef(dbms);
				trigDef.setName(match.group(1));
				trigDef.setSource(match.group(0));
				metadata.addTrigger(trigDef);
			}
		}
		return metadata;
	}

	@Override
	public void writeMetadata(SchemaMetaData metadata, OutputStream out) throws JDOException, IOException {
		try (OutputStreamWriter wrtr = new OutputStreamWriter(out)) {
			for (ExtendableDef objDef : metadata.all()) {
				Scriptable srcDef = (Scriptable) objDef;
				wrtr.write(srcDef.getSource());
				wrtr.write(getstatementDelimiter());
			}
		}
	}

}
