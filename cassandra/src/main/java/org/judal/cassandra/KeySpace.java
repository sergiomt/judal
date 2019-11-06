package org.judal.cassandra;

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

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;
import javax.jdo.metadata.ColumnMetadata;
import javax.transaction.TransactionManager;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.factory.HFactory;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createCounterColumn;
import static me.prettyprint.hector.api.factory.HFactory.createColumnFamilyDefinition;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.IndexDef;
import org.judal.metadata.IndexDef.Type;
import org.judal.metadata.IndexDef.Using;
import org.judal.metadata.JoinType;
import org.judal.metadata.NameAlias;
import org.judal.metadata.NonUniqueIndexDef;
import org.judal.storage.table.IndexableTable;
import org.judal.storage.table.IndexableView;
import org.judal.storage.table.Record;
import org.judal.storage.table.TableDataSource;
import org.judal.storage.table.Table;
import org.judal.storage.table.View;

import com.knowgate.tuples.Pair;

import org.judal.storage.FieldHelper;
import org.judal.storage.Param;
import org.judal.metadata.SchemaMetaData;
import org.judal.metadata.TableDef;
import org.judal.metadata.UniqueIndexDef;
import org.judal.metadata.ViewDef;

public class KeySpace implements TableDataSource {

	private Map<String, String> oProps;
	private Cluster oCassandraCluster;
	private SchemaMetaData oSch;
	private String sKeySpaceName;
	private Keyspace oKeySpace;
	private boolean bReadOnlySchema;
	private boolean bIsClosed;
	private FieldHelper oFldHlpr;

	private static Pattern oUrlPatt = Pattern.compile("\\w+://([\\w|\\x2E]+)(:\\d+)?/(\\w+)");

	public KeySpace() {
		bIsClosed = true;
		oSch = null;
		oProps = null;
		oFldHlpr = null;
	}

	public KeySpace(Map<String, String> properties) {
		oSch = null;
		oProps = properties;
		oFldHlpr = null;
		open(properties.get("dburl"), properties.getOrDefault("dbuser", ""), properties.getOrDefault("dbpassword", ""),
				false);
	}

	public KeySpace(Map<String, String> properties, SchemaMetaData metaData) {
		oSch = metaData;
		oProps = properties;
		oFldHlpr = null;
		open(properties.get("dburl"), properties.getOrDefault("dbuser", ""), properties.getOrDefault("dbpassword", ""),
				false);
	}

	public void open(String sUrl, String sUser, String sPassw, boolean bReadOnly) throws JDOException {
		Matcher oMatt = oUrlPatt.matcher(sUrl);
		oMatt.matches();
		String sClusterName = oMatt.group(1);
		String sClusterMachineAndPort = sClusterName + oMatt.group(2);
		oCassandraCluster = HFactory.getOrCreateCluster(sClusterName, sClusterMachineAndPort);
		sKeySpaceName = oMatt.group(3);
		bReadOnlySchema = bReadOnly;
		bIsClosed = false;
		oKeySpace = createKeyspace(sKeySpaceName, oCassandraCluster);
	}

	@Override
	public void close() throws JDOException {
		bIsClosed = true;
	}

	public boolean isReadOnly() {
		return bReadOnlySchema;
	}

	public boolean isClosed() {
		return bIsClosed;
	}

	public String getName() {
		return sKeySpaceName;
	}

	public ThriftCluster getCluster() {
		return (ThriftCluster) oCassandraCluster;
	}

	private TableDef readTable(ColumnFamilyDefinition oCfDef) {
		TableDef oTdef = createTableDef(oCfDef.getName(), new HashMap<String, Object>());
		for (ColumnDefinition oCDef : oCfDef.getColumnMetadata()) {
			boolean bIsNullable, bIsIndexed, bIsPrimaryKey;
			ColumnIndexType oIxTp = oCDef.getIndexType();
			if (oIxTp == null) {
				bIsNullable = true;
				bIsIndexed = bIsPrimaryKey = false;
			} else if (oIxTp.equals(ColumnIndexType.KEYS)) {
				bIsNullable = false;
				bIsIndexed = true;
				bIsPrimaryKey = false;
			} else {
				bIsNullable = true;
				bIsIndexed = true;
				bIsPrimaryKey = false;
			}
			String sTypeName = oCDef.getValidationClass();
			sTypeName = sTypeName.substring(sTypeName.lastIndexOf('.') + 1);
			bIsPrimaryKey = sTypeName.equals("UUIDType");
			if (oCfDef.getColumnType().equals(ColumnType.STANDARD)) {
				if (bIsPrimaryKey)
					oTdef.addPrimaryKeyColumn(null, StringSerializer.get().fromByteBuffer(oCDef.getName()),
							ColumnDef.getSQLType(sTypeName));
				else
					oTdef.addColumnMetadata(null, StringSerializer.get().fromByteBuffer(oCDef.getName()),
							ColumnDef.getSQLType(sTypeName), bIsNullable,
							bIsIndexed ? NonUniqueIndexDef.Type.ONE_TO_MANY : null);
			} else {
				oTdef.addColumnMetadata(null, oCDef.getName().toString(), Types.ARRAY, bIsNullable,
						bIsIndexed ? NonUniqueIndexDef.Type.ONE_TO_MANY : null);
			}
		} // next
		return oTdef;
	}

	@Override
	public SchemaMetaData getMetaData() throws JDOException {
		if (null == oSch) {
			oSch = new SchemaMetaData();
			try {
				KeyspaceDefinition oKsDef = oCassandraCluster.describeKeyspace(sKeySpaceName);
				for (ColumnFamilyDefinition oCfDef : oKsDef.getCfDefs())
					oSch.addTable(readTable(oCfDef), null);
			} catch (HectorException hcpt) {
				throw new JDOException(hcpt.getMessage(), hcpt);
			}
		} // fi
		return oSch;
	}

	@Override
	public void setMetaData(SchemaMetaData oSmd) {
		oSch = oSmd;
	}

	@Override
	public void createTable(TableDef tableDef, Map<String, Object> options) throws JDOException {
		BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
		columnFamilyDefinition.setKeyspaceName(sKeySpaceName);
		columnFamilyDefinition.setName(tableDef.getName());
		for (ColumnDef oCol : tableDef.getColumns()) {
			BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
			columnDefinition.setName(StringSerializer.get().toByteBuffer(oCol.getName()));
			if (oCol.isPrimaryKey()) {
				columnDefinition.setValidationClass(ComparatorType.UUIDTYPE.getClassName());
				columnFamilyDefinition.addColumnDefinition(columnDefinition);

			} else {
				if (oCol.isIndexed())
					columnDefinition.setIndexType(ColumnIndexType.KEYS);
				String sValidationClass = null;
				switch (oCol.getType()) {
				case Types.SMALLINT:
					sValidationClass = ComparatorType.INTEGERTYPE.getClassName();
					break;
				case Types.INTEGER:
					sValidationClass = ComparatorType.INT32TYPE.getClassName();
					break;
				case Types.BIGINT:
					sValidationClass = ComparatorType.LONGTYPE.getClassName();
					break;
				case Types.TIMESTAMP:
					sValidationClass = ComparatorType.DATETYPE.getClassName();
					break;
				case Types.BOOLEAN:
					sValidationClass = ComparatorType.BOOLEANTYPE.getClassName();
					break;
				case Types.DECIMAL:
				case Types.NUMERIC:
					sValidationClass = ComparatorType.DECIMALTYPE.getClassName();
					break;
				case Types.CHAR:
				case Types.NCHAR:
				case Types.VARCHAR:
				case Types.NVARCHAR:
				case Types.LONGVARCHAR:
				case Types.LONGNVARCHAR:
					sValidationClass = ComparatorType.UTF8TYPE.getClassName();
					break;
				case Types.NULL:
					throw new JDOException("Column " + oCol.getName() + " has NULL type");
				case Types.BLOB:
				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
				case Types.JAVA_OBJECT:
					sValidationClass = ComparatorType.BYTESTYPE.getClassName();
					break;
				default:
					throw new JDOException(
							"Could not assign validation class for type " + ColumnDef.typeName(oCol.getType()));
				}
				columnDefinition.setValidationClass(sValidationClass);
				columnFamilyDefinition.addColumnDefinition(columnDefinition);
			}
		}
		oCassandraCluster.addColumnFamily(new ThriftCfDef(columnFamilyDefinition));
	}

	@Override
	public Table openTable(Record oRec) throws JDOException {
		return new ColumnFamily(this, oRec.getTableName());
	}

	public Keyspace keySpace() {
		return oKeySpace;
	}

	private ColumnFamilyDefinition getCountersFamily() {
		KeyspaceDefinition oKsDef = oCassandraCluster.describeKeyspace(sKeySpaceName);
		ColumnFamilyDefinition oCountersFamily = null;
		for (ColumnFamilyDefinition oCfDef : oKsDef.getCfDefs()) {
			if (oCfDef.getName().equalsIgnoreCase("counters")) {
				oCountersFamily = oCfDef;
				break;
			}
		} // next
		if (oCountersFamily == null) {
			oCountersFamily = createColumnFamilyDefinition(sKeySpaceName, "counters");
			oCountersFamily.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());
			oCassandraCluster.addColumnFamily(oCountersFamily);
		}
		return oCountersFamily;
	}

	private ColumnDefinition getCounterDefinition(String sSequenceName) {
		ColumnFamilyDefinition oCountersFamily = getCountersFamily();
		ColumnDefinition oCounterDef = null;
		for (ColumnDefinition oCDef : oCountersFamily.getColumnMetadata()) {
			if (oCDef.getName().toString().equalsIgnoreCase(sSequenceName)) {
				oCounterDef = oCDef;
				break;
			}
		}
		return oCounterDef;
	}

	private BasicColumnDefinition createCounterDefinition(String sSequenceName) {
		ColumnFamilyDefinition oCountersFamily = getCountersFamily();
		BasicColumnDefinition oBasicDef = new BasicColumnDefinition();
		oBasicDef.setName(StringSerializer.get().toByteBuffer(sSequenceName));
		oBasicDef.setIndexType(ColumnIndexType.KEYS);
		oBasicDef.setValidationClass(ComparatorType.COUNTERTYPE.getClassName());
		oCountersFamily.addColumnDefinition(oBasicDef);
		oCassandraCluster.updateColumnFamily(new ThriftCfDef(oCountersFamily));
		return oBasicDef;
	}

	@Override
	public Object call(String sCmd, Param... aParams) throws JDOException {
		throw new JDOUnsupportedOptionException("Cassandra does not support method calls");
	}

	@Override
	public boolean exists(String tableName, String objectType) throws JDOException {
		if (null == tableName)
			throw new NullPointerException("KeySpace.exists() table name cannot be null");
		if ("U".equals(objectType)) {
			if (null == oSch) {
				KeyspaceDefinition oKsDef = oCassandraCluster.describeKeyspace(sKeySpaceName);
				for (ColumnFamilyDefinition oCfDef : oKsDef.getCfDefs())
					if (oCfDef.getName().equalsIgnoreCase(tableName))
						return true;
				return false;
			} else {
				return oSch.getTable(tableName) !=null;
			}
		} else {
			return false;
		}
	}

	@Override
	public Map<String, String> getProperties() {
		return oProps;
	}

	@Override
	public TransactionManager getTransactionManager() {
		return null;
	}

	@Override
	public JDOConnection getJdoConnection() throws JDOException {
		return null;
	}

	@Override
	public CSSequence getSequence(String name) throws JDOException {
		if (bReadOnlySchema)
			throw new JDOException("Cannot create sequence because the KeySpace is in read only mode");
		Mutator<String> oMtr = createMutator(oKeySpace, StringSerializer.get());
		ColumnDefinition oCounterDef = getCounterDefinition(name);
		if (oCounterDef == null) {
			oCounterDef = createCounterDefinition(name);
			oMtr.insertCounter("kounter", "counters", createCounterColumn(name, 0l));
		}
		return new CSSequence(name, oKeySpace, oMtr);
	}

	@Override
	public boolean inTransaction() throws JDOException {
		return false;
	}

	@Override
	public TableDef getTableDef(String tableName) throws JDOException {
		if (null == tableName)
			throw new NullPointerException("KeySpace.getTableDef() table name cannot be null");

		TableDef retval = null;
		if (null == oSch) {
			KeyspaceDefinition oKsDef = oCassandraCluster.describeKeyspace(sKeySpaceName);
			for (ColumnFamilyDefinition oCfDef : oKsDef.getCfDefs())
				if (oCfDef.getName().equalsIgnoreCase(tableName))
					retval = readTable(oCfDef);
		} else {
			retval = oSch.getTable(tableName);
		}
		return retval;
	}

	@Override
	public ViewDef getViewDef(String viewName) throws JDOException {
		return getTableDef(viewName);
	}

	@Override
	public ViewDef getTableOrViewDef(String objectName) throws JDOException {
		return getTableDef(objectName);
	}

	@Override
	/**
	 * Create ColumnDef
	 * @param columnName String Column Name
	 * @param position int [1..n]
	 * @param colType short One of java.sql.Types
	 * @param options Map&lt;String, Object&gt; Can be null. If not null and contains key "family" then the given family name is set on the ColumnDef 
	 * @return ColumnDef
	 * @throws JDOException
	 */
	public ColumnDef createColumnDef(String columnName, int position, short colType, Map<String, Object> options)
		throws JDOException {
		ColumnDef cdef = new ColumnDef(columnName, position, colType);
		if (options!=null) {
			if (options.containsKey("family"))
				cdef.setFamily((String) options.get("family"));
			else if (options.containsKey("familyName"))
				cdef.setFamily((String) options.get("familyName"));
		}
		return cdef;
	}

	@Override
	public TableDef createTableDef(String tableName, Map<String, Object> options) throws JDOException {
		return new TableDef(tableName);
	}

	@Override
	public IndexDef createIndexDef(String indexName, String tableName, Iterable<String> columns, Type indexType, Using using)
		throws JDOException {
		ArrayList<ColumnMetadata> indexColumns = new ArrayList<>();
		int position = 0;
		for (String columnName : columns)
			indexColumns.add(createColumnDef(columnName, ++position, (short) java.sql.Types.NULL, null));
		return IndexDef.Type.ONE_TO_ONE.equals(indexType) ? 
			new UniqueIndexDef(tableName, indexName, indexColumns.toArray(new ColumnMetadata[indexColumns.size()]), indexType, using) :
			new NonUniqueIndexDef(tableName, indexName, indexColumns.toArray(new ColumnMetadata[indexColumns.size()]), indexType, using);
	}

	@Override
	public void dropTable(String tableName, boolean cascade) throws JDOException {
		try {
			oCassandraCluster.dropColumnFamily(sKeySpaceName, tableName);
		} catch (HectorException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public void truncateTable(String tableName, boolean cascade) throws JDOException {
		try {
			oCassandraCluster.truncate(sKeySpaceName, tableName);;
		} catch (HectorException e) {
			throw new JDOException(e.getMessage(), e);
		}
	}

	@Override
	public IndexableTable openIndexedTable(Record recordInstance) throws JDOException {
		return new ColumnFamily(this, recordInstance.getTableName());
	}

	@Override
	public View openView(Record recordInstance) throws JDOException {
		return new ColumnFamily(this, recordInstance.getTableName());
	}

	@Override
	public IndexableView openIndexedView(Record recordInstance) throws JDOException {
		return new ColumnFamily(this, recordInstance.getTableName());
	}

	@Override
	public IndexableView openJoinView(JoinType joinType, Record result, NameAlias baseTable, NameAlias joinedTable,
		Pair<String, String>... onColumns) throws JDOException {
		throw new JDOUnsupportedOptionException("Cassandra does not support join views");
	}

	@Override
	public FieldHelper getFieldHelper() throws JDOException {
		return oFldHlpr;
	}

	public void setFieldHelper(FieldHelper fieldHelper) {
		oFldHlpr = fieldHelper;
	}
}
