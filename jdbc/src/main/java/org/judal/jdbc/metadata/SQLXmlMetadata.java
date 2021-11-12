package org.judal.jdbc.metadata;

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

import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import javax.jdo.JDOException;
import javax.jdo.metadata.ColumnMetadata;
import javax.jdo.metadata.ForeignKeyMetadata;
import javax.jdo.metadata.IndexMetadata;

import static javax.jdo.annotations.ForeignKeyAction.CASCADE;

import org.judal.jdbc.JDBCTableDataSource;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.ForeignKeyDef;
import org.judal.metadata.MetadataScanner;
import org.judal.metadata.PrimaryKeyDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;

import com.pureperfect.ferret.ScanFilter;
import com.pureperfect.ferret.Scanner;
import com.pureperfect.ferret.vfs.PathElement;

import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.UniqueIndex;
import org.apache.ddlutils.model.NonUniqueIndex;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.Reference;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.CascadeActionEnum;

public class SQLXmlMetadata implements MetadataScanner {

	private JDBCTableDataSource dts;

	public SQLXmlMetadata(JDBCTableDataSource dts) {
		this.dts = dts;
	}

	protected final ScanFilter all = new ScanFilter() {
		@Override
		public boolean accept(final PathElement resource) {
			return true;
		}
	};

	public InputStream openStream(String packagePath, String xmlFileName) throws JDOException, IOException {
		ClassLoader cldr = Thread.currentThread().getContextClassLoader();
		Scanner scn = new Scanner();
		URL urlResource = cldr.getResource(packagePath + "/" + xmlFileName);
		if (null == urlResource)
			throw new FileNotFoundException("Resource not found " + packagePath + "/" + xmlFileName);
		scn.add(urlResource);
		Set<? extends PathElement> elements = scn.scan(all);
		return elements.iterator().next().openStream();
	}

	@Override
	public SchemaMetaData readMetadata(InputStream in) throws JDOException, IOException {
		Database db;
		DatabaseIO dio = new DatabaseIO();
		SchemaMetaData metadata = new SchemaMetaData();
		InputStreamReader rdr = new InputStreamReader(in);
		db = dio.read(rdr);
		rdr.close();
		Map<String,Object> opts = new HashMap<>();
		for (int t = 0; t < db.getTableCount(); t++) {
			Table tbl = db.getTable(t);
			SQLTableDef tdef = dts.createTableDef(tbl.getName(), opts);
			PrimaryKeyDef pdef = new PrimaryKeyDef();
			tdef.setSchema(tbl.getSchema());
			tdef.setCatalog(tbl.getCatalog());
			tdef.setDescription(tbl.getDescription());
			for (int c = 0; c < tbl.getColumnCount(); c++) {
				Column col = tbl.getColumn(c);
				tdef.addColumnMetadata(null, col.getName(), col.getTypeCode(), col.getSizeAsInt(), col.getScale(),
						!col.isRequired(), null, null, col.getDefaultValue(), col.isPrimaryKey());
				if (col.isPrimaryKey())
					pdef.addColumn(tdef.getColumnByName(col.getName()));
			}
			if (tbl.hasPrimaryKey())
				tdef.setPrimaryKeyMetadata(pdef);
			for (Column acol : tbl.getAutoIncrementColumns())
				tdef.getColumnByName(acol.getName()).setAutoIncrement(true);
			for (ForeignKey fk : tbl.getForeignKeys()) {
				ForeignKeyDef fdef = new ForeignKeyDef();
				fdef.setName(fk.getName());
				fdef.setTable(fk.getForeignTableName());
				for (Reference ref : fk.getReferences()) {
					ColumnDef cdef = tdef.getColumnByName(ref.getLocalColumnName());
					cdef.setTargetField(ref.getForeignColumnName());
					cdef.setTarget(fk.getForeignTableName());
					fdef.addColumn(cdef);
				}
				tdef.addForeignKeyMetadata(fdef);
			}
			
			metadata.addTable(tdef, null);
			
			for (Index idx : tbl.getIndices()) {
				LinkedList<ColumnMetadata> cols = new LinkedList<>();
				for (IndexColumn icol : idx.getColumns())
					cols.add(tdef.getColumnByName(icol.getName()));
				String[] colNames = new String[cols.size()];
				int c = 0;
				for (ColumnMetadata col : cols)
					colNames[c++] = col.getName();
				metadata.addIndex(new SQLIndex(tbl.getName(), idx.getName(), colNames, idx.isUnique()));
			}
		}
		return metadata;
	}

	@Override
	public void writeMetadata(SchemaMetaData metadata, OutputStream out) throws JDOException, IOException {
		Database db = new Database();

		for (TableDef tdef : metadata.tables()) {
			Table tbl = new Table();

			tbl.setCatalog(tdef.getCatalog());
			tbl.setSchema(tdef.getSchema());
			tbl.setName(tdef.getName());
			tbl.setDescription(tdef.getDescription());

			for(ColumnDef cdef : tdef.getColumns()) {
				Column col = new Column();
				col.setAutoIncrement(cdef.getAutoIncrement());
				col.setDefaultValue(cdef.getDefaultValue());
				col.setDescription(cdef.getDescription());
				col.setName(cdef.getName());
				col.setScale(cdef.getScale());
				col.setRequired(!cdef.getAllowsNull());
				col.setSize(String.valueOf(cdef.getLength()));
				col.setType(cdef.getSQLType());
				col.setTypeCode(cdef.getType());
				tbl.addColumn(col);
			}

			PrimaryKeyDef pk = tdef.getPrimaryKeyMetadata();
			if (pk!=null) {
				Index idx = new UniqueIndex();
				idx.setName(pk.getName());
				int p = 0;
				for (ColumnDef pkdef : pk.getColumns()) {
					IndexColumn idxcol = new IndexColumn();
					idxcol.setOrdinalPosition(++p);
					idxcol.setColumn(tbl.findColumn(pkdef.getName()));
					idxcol.setName(pkdef.getName());
					idxcol.setSize(String.valueOf(pkdef.getLength()));
					idx.addColumn(idxcol);
				}
				tbl.addIndex(idx);
			}

			for (ForeignKeyMetadata fkmdat : tdef.getForeignKeys()) {
				ForeignKey fk = new ForeignKey();
				fk.setName(fkmdat.getName());
				fk.setOnDelete(fkmdat.getDeleteAction()!=null ? CASCADE.equals(fkmdat.getDeleteAction()) ? CascadeActionEnum.CASCADE : CascadeActionEnum.NONE : CascadeActionEnum.SET_DEFAULT);
				fk.setOnUpdate(fkmdat.getUpdateAction()!=null ? CASCADE.equals(fkmdat.getUpdateAction()) ? CascadeActionEnum.CASCADE : CascadeActionEnum.NONE : CascadeActionEnum.SET_DEFAULT);
				fk.setForeignTableName(fkmdat.getTable());
				int s = 0;
				for (ColumnMetadata colmdat : fkmdat.getColumns()) {
					Reference ref= new Reference();
					ref.setSequenceValue(++s);
					ref.setLocalColumnName(colmdat.getName());
					ref.setForeignColumnName(colmdat.getTarget());
					fk.addReference(ref);
				}
				tbl.addForeignKey(fk);
			}

			for (IndexMetadata idxmdat : tdef.getIndices()) {
				Index idx = idxmdat.getUnique() ? new UniqueIndex() : new NonUniqueIndex();
				idx.setName(idxmdat.getName());
				int p = 0;
				for (ColumnMetadata colmdat : idxmdat.getColumns()) {
					IndexColumn idxcol = new IndexColumn();
					idxcol.setOrdinalPosition(++p);
					idxcol.setColumn(tbl.findColumn(colmdat.getName()));
					idxcol.setName(colmdat.getName());
					idxcol.setSize(String.valueOf(colmdat.getLength()));
					idx.addColumn(idxcol);
				}
				tbl.addIndex(idx);

				try (OutputStreamWriter wrtr = new OutputStreamWriter(out)) {
					new DatabaseIO().write(db, wrtr);
				}
			}
		}
	}

}
