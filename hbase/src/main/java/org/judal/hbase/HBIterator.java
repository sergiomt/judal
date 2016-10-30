package org.judal.hbase;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.io.IOException;
import java.util.Iterator;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;
import org.judal.storage.Record;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.Stored;
import org.judal.serialization.BytesConverter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBIterator implements AutoCloseable, Iterator<Stored> {

	private HBTable table;
	private ResultScanner scanner;
	private Result result;

	public HBIterator(HBTable table) throws JDOException {
		this.table = table;
		Scan scan = new Scan();
		for (ColumnDef cdef : table.columns()) {
			scan.addColumn(BytesConverter.toBytes(cdef.getFamily()), BytesConverter.toBytes(cdef.getName()));
		}
		try {
			scanner = table.getTable().getScanner(scan);
		} catch (IOException ioe) {
			throw new JDOException (ioe.getMessage(), ioe);
		}
	}

	@Override
	public boolean hasNext() throws JDOException {
		try {
			result = scanner.next();
		} catch (IOException ioe) {
			throw new JDOException (ioe.getMessage(), ioe);
		}
		return false;
	}

	@Override
	public Record next() throws JDOException {
		Record rec;
		try {
			rec = StorageObjectFactory.newRecord(table.getResultClass(), table.getDataSource().getTableDef(table.name()));
		} catch (NoSuchMethodException e) {
			throw new JDOException("StorageObjectFactory.newRecord() cannot find a suitable constructor for HBase Record");
		}
		try {
			for (ColumnDef cdef : table.columns()) {
				KeyValue kv = result.getColumnLatest(BytesConverter.toBytes(cdef.getFamily()), BytesConverter.toBytes(cdef.getName()));
				if (kv!=null)
					if (kv.getValue()!=null)
						rec.put(cdef.getName(), BytesConverter.fromBytes(kv.getValue(), cdef.getType()));
			}
		} catch (IOException ioe) {
			throw new JDOException (ioe.getMessage(), ioe);			
		}
		return rec;
	}

	@Override
	public void close() {
		scanner.close();
	}

}
