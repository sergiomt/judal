package org.judal.jdbc.metadata;

import java.io.InputStreamReader;
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

import org.judal.jdbc.JDBCTableDataSource;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.ForeignKeyDef;
import org.judal.metadata.NonUniqueIndexDef;
import org.judal.metadata.MetadataScanner;
import org.judal.metadata.PrimaryKeyDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.UniqueIndexDef;
import org.judal.metadata.IndexDef.Type;

import com.pureperfect.ferret.ScanFilter;
import com.pureperfect.ferret.Scanner;
import com.pureperfect.ferret.vfs.PathElement;

import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.Reference;
import org.apache.ddlutils.model.Table;

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
			for (Index idx : tbl.getIndices()) {
				LinkedList<ColumnMetadata> cols = new LinkedList<ColumnMetadata>();
				for (IndexColumn icol : idx.getColumns())
					cols.add(tdef.getColumnByName(icol.getName()));
				if (idx.isUnique())
					metadata.addIndex(new UniqueIndexDef(tbl.getName(), idx.getName(), cols, Type.ONE_TO_ONE));
				else
					metadata.addIndex(new NonUniqueIndexDef(tbl.getName(), idx.getName(), cols, Type.ONE_TO_MANY));
			}
			metadata.addTable(tdef);
		}
		return metadata;
	}

}
