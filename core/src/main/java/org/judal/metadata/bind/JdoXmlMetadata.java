package org.judal.metadata.bind;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;

import javax.jdo.JDOException;

import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;

import org.judal.metadata.ClassPackage;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.ForeignKeyDef;
import org.judal.metadata.MemberGroupDef;
import org.judal.metadata.MetadataScanner;
import org.judal.metadata.PrimaryKeyDef;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.storage.DataSource;
import org.judal.storage.table.TableDataSource;

import com.sun.java.xml.ns.jdo.jdo.Jdo;
import com.sun.java.xml.ns.jdo.jdo._Class;
import com.sun.java.xml.ns.jdo.jdo._Package;
import com.sun.java.xml.ns.jdo.jdo.Extension;
import com.sun.java.xml.ns.jdo.jdo.AttlistExtension;
import com.sun.java.xml.ns.jdo.jdo.AttlistColumn;
import com.sun.java.xml.ns.jdo.jdo.AttlistColumn.AllowsNull;
import com.sun.java.xml.ns.jdo.jdo.AttlistForeignKey.Deferred;

import com.knowgate.debug.DebugFile;

/**
 * <p>Load schema metadata from a JDO XML file.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class JdoXmlMetadata implements MetadataScanner {

	private DataSource dts;

	/**
	 * Constructor
	 * @param dts DataSource
	 */
	public JdoXmlMetadata (DataSource dts) {
		this.dts = dts;
	}

	/**
	 * <p>Write schema definition as JDO XML.</p>
	 * @param metadata SchemaMetaData
	 * @param out OutputStream
	 * @throws JDOException
	 * @throws IOException
	 */
	public void writeMetadata(SchemaMetaData metadata, OutputStream out) throws JDOException, IOException {
		OutputStreamWriter writer = new OutputStreamWriter(out);
		writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		writer.write("<jdo xmlns=\"http://java.sun.com/xml/ns/jdo/jdo\" schema=\""+metadata.getSchema()+"\">\n");
		for (ClassPackage pckg : metadata.packages()) {
			writer.write(pckg.toJdoXml());
			writer.write("\n");
		}
		writer.write("</jdo>");
		writer.close();
	}
	
	private TableDef createTableDef(String tableName) {
		if (null==dts)
			return new TableDef(tableName);
		else if (dts instanceof TableDataSource)
			return ((TableDataSource) dts).createTableDef(tableName, new HashMap<String,Object>());
		else
			return new TableDef(tableName);
	}

	/**
	 * <p>Create schema metadata from JDO XML description.</p>
	 * @param in InputStream 
	 * @return SchemaMetaData
	 * @throws JDOException
	 * @throws IOException
	 * @throws NullPointerException if InputStream is <b>null</b>
	 */
	@Override
	public SchemaMetaData readMetadata(InputStream in) throws JDOException, IOException, NullPointerException {

		if (DebugFile.trace) {
			DebugFile.writeln("Begin JdoXmlMetadata.readMetadata(InputStream)");
			DebugFile.incIdent();
		}
	
		if (null==in) {
			DebugFile.writeln("NullPointerException InputStream cannot be null");
			DebugFile.decIdent();
			throw new NullPointerException("JdoXmlMetadata.readMetadata() InputStream cannot be null");
		}
		
		SchemaMetaData metadata = new SchemaMetaData();

		Jdo jdo;
		try {
			IBindingFactory bfact = BindingDirectory.getFactory(Jdo.class);
		    IUnmarshallingContext uctx = bfact.createUnmarshallingContext();
		    jdo = (Jdo) uctx.unmarshalDocument (in, "UTF8");
		} catch (JiBXException jibx) {
			throw new JDOException(jibx.getMessage(), jibx);
		}				

	    String schema = jdo.getAttlistJdo().getSchema();
		String catalog = jdo.getAttlistJdo().getCatalog();

		if (DebugFile.trace)
			DebugFile.writeln("schema="+schema+" catalog="+catalog);

		if (schema!=null)
			metadata.setSchema(schema);
		if (catalog!=null)
			metadata.setCatalog(catalog);

		for (com.sun.java.xml.ns.jdo.jdo.Jdo.Choice p : jdo.getChoiceList()) {
	    	_Package pack = p.getPackage();
			if (DebugFile.trace)
				DebugFile.writeln("package="+pack.getAttlistPackage().getName());
	    	ClassPackage pckg = metadata.addPackage(pack.getAttlistPackage().getName());
	    	HashMap<String,Object> options = new HashMap<String,Object>();
	    	for (com.sun.java.xml.ns.jdo.jdo._Package.Choice d : pack.getChoiceList()) {
	    		if (d.if_Class()) {
		    		_Class clss = d.get_Class();
					if (DebugFile.trace)
						DebugFile.writeln("class="+clss.getAttlistClass().getName()+" table="+clss.getAttlistClass().getTable());
			    	TableDef tbl = createTableDef(clss.getAttlistClass().getTable());
			    	tbl.setRecordClassName(pack.getAttlistPackage().getName()+"."+clss.getAttlistClass().getName());
			    	List<com.sun.java.xml.ns.jdo.jdo._Class.Choice> clssChoices = clss.getChoiceList();
			    	if (clssChoices!=null) {
				    	for (com.sun.java.xml.ns.jdo.jdo._Class.Choice c : clssChoices) {
			    			if (c.ifColumn()) {
			    				final AttlistColumn col = c.getColumn().getAttlistColumn();
			    				final String cname = col.getName().trim();
			    				final String tname = col.getJdbcType().trim();
			    				final String clength = col.getLength();
			    				final String cscale = col.getScale();
			    				final String dvalue = col.getDefaultValue();
			    				final String target = col.getTarget();
			    				final String targetField = col.getTargetField();
			    				final boolean nullable = col.getAllowsNull()==null ? true : col.getAllowsNull().equals(AllowsNull.TRUE);
			    				final int ctype = ColumnDef.getSQLType(tname);
								if (DebugFile.trace)
									DebugFile.writeln("column="+cname+" "+tname+"("+clength+","+cscale+") "+(nullable ? "null" : "not null")+" default "+dvalue);
			    				switch (ctype) {
			    				case Types.CHAR:
			    				case Types.NCHAR:
			    				case Types.VARCHAR:
			    				case Types.NVARCHAR:
			    					tbl.addColumnMetadata("", cname, ctype, Integer.parseInt(clength), 0, nullable, null, null, dvalue, false);
			    					break;
			    				case Types.DECIMAL:
			    					tbl.addColumnMetadata("", cname, ctype, Integer.parseInt(clength), Integer.parseInt(cscale), nullable, null, null, dvalue, false);
			    					break;
			    				case Types.CLOB:
			    				case Types.BLOB:
			    				case Types.LONGVARCHAR:
			    				case Types.LONGNVARCHAR:
			    				case Types.LONGVARBINARY:
			    					if (clength==null || clength.length()==0)
			    						tbl.addColumnMetadata("", cname, ctype, ColumnDef.getDefaultPrecision(ctype), 0, nullable, null, null, dvalue, false);
			    					else
			    						tbl.addColumnMetadata("", cname, ctype, Integer.parseInt(clength), 0, nullable, null, null, dvalue, false);
			    					break;
			    				default:
			    					tbl.addColumnMetadata("", cname, ctype, nullable);
			    					tbl.getColumnByName(cname).setDefaultValue(dvalue);
			    				}
			    				final ColumnDef newcol = tbl.getColumnByName(cname);
			    				if (target!=null && target.trim().length()>0)
			    					newcol.setTarget(target);
			    				if (targetField!=null && targetField.trim().length()>0)
			    					newcol.setTargetField(targetField);
			    				List<Extension> extensions = c.getColumn().getExtensionList();
			    				if (extensions!=null)
			    					for (Extension ext : extensions) {
			    						AttlistExtension keyval = ext.getAttlistExtension();
			    						if (keyval.getKey().equalsIgnoreCase("family"))
			    							newcol.setFamily(keyval.getValue());
			    					}
			    			}
			    			if (c.ifPrimaryKey()) {
			    				PrimaryKeyDef pk = new PrimaryKeyDef();
			    				pk.setName(c.getPrimaryKey().getAttlistPrimaryKey().getName());
								if (DebugFile.trace)
									DebugFile.writeln("primary key="+pk.getName());
			    				String pkcols = c.getPrimaryKey().getAttlistPrimaryKey().getColumn();
			    				if (pkcols!=null && pkcols.length()>0)
			    					for (String pkcol : pkcols.split(","))
			    						if (pkcol.trim().length()>0)
			    							pk.addColumn(tbl.getColumnByName(pkcol));
			    				List<com.sun.java.xml.ns.jdo.jdo.PrimaryKey.Choice> pkcollist = c.getPrimaryKey().getChoiceList();
			    				if (pkcollist!=null)
			    					for (com.sun.java.xml.ns.jdo.jdo.PrimaryKey.Choice pkcol : pkcollist)
			    						if (pkcol.ifColumn())
			    							pk.addColumn(tbl.getColumnByName(pkcol.getColumn().getAttlistColumn().getName()));
		    					tbl.setPrimaryKeyMetadata(pk);
			    			}
			    			if (c.ifForeignKey()) {
			    				String fkname = c.getForeignKey().getAttlistForeignKey().getName();
			    				String ftable = c.getForeignKey().getAttlistForeignKey().getTable();
								if (DebugFile.trace)
									DebugFile.writeln("foreign key key="+ftable+"."+fkname);
			    				boolean deferred;
			    				if (c.getForeignKey().getAttlistForeignKey().getDeferred()==null)
			    					deferred = false;
			    				else
			    					deferred = c.getForeignKey().getAttlistForeignKey().getDeferred().equals(Deferred.TRUE);
			    				ForeignKeyDef fkdef = new ForeignKeyDef();
			    				fkdef.setName(fkname);
			    				fkdef.setTable(ftable);
			    				fkdef.setDeferred(deferred);
			    				for (com.sun.java.xml.ns.jdo.jdo.ForeignKey.Choice fkchoice : c.getForeignKey().getChoiceList())
			    					if (fkchoice.ifChoice())
			    						if (fkchoice.getChoice().ifColumnList())
					    					for (com.sun.java.xml.ns.jdo.jdo.Column fkcol : fkchoice.getChoice().getColumnList())
					    						fkdef.addColumn(tbl.getColumnByName(fkcol.getAttlistColumn().getName()));
			    				tbl.addForeignKeyMetadata(fkdef);
			    			}
			    			if (c.ifFetchGroup()) {
			    				String fgname = c.getFetchGroup().getAttlistFetchGroup().getName();
			    				MemberGroupDef fgroup = tbl.newFetchGroupMetadata(fgname);
			    				for (com.sun.java.xml.ns.jdo.jdo.FetchGroup.Choice fgchoice : c.getFetchGroup().getChoiceList())
			    					if (c.ifField())
					    				fgroup.newFieldMetadata(fgchoice.getField().getAttlistField().getColumn());			    						
			    			}
			    		} // next			    		
			    	}
			    	metadata.addTable(tbl, pckg.getName());
	    		}
	    	} // next
	    }

		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JdoXmlMetadata.readMetadata(InputStream)");
		}
		
		return metadata;
	}
}
