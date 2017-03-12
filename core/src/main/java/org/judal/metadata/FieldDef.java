package org.judal.metadata;

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

import java.sql.Types;

import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.annotations.ForeignKeyAction;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.NullValue;
import javax.jdo.annotations.PersistenceModifier;
import javax.jdo.metadata.ArrayMetadata;
import javax.jdo.metadata.CollectionMetadata;
import javax.jdo.metadata.ElementMetadata;
import javax.jdo.metadata.EmbeddedMetadata;
import javax.jdo.metadata.ForeignKeyMetadata;
import javax.jdo.metadata.IndexMetadata;
import javax.jdo.metadata.JoinMetadata;
import javax.jdo.metadata.KeyMetadata;
import javax.jdo.metadata.MapMetadata;
import javax.jdo.metadata.MemberMetadata;
import javax.jdo.metadata.FieldMetadata;
import javax.jdo.metadata.OrderMetadata;
import javax.jdo.metadata.UniqueMetadata;
import javax.jdo.metadata.ValueMetadata;

/**
 * JDO FieldMetadata interface implementation
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class FieldDef extends BaseDef implements FieldMetadata {

	private static final long serialVersionUID = 10000l;

	private boolean serialized;
	private boolean defaultFetchGroup;

	public FieldDef() {
		serialized = true;
		defaultFetchGroup = true;
	}

	public FieldDef(FieldDef source) {
		super(source);
		this.serialized = source.serialized;
	}

	@Override
	public FieldDef clone() {
		return new FieldDef(this);
	}
	
	@Override
	public ArrayMetadata getArrayMetadata() throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("BaseDef does not support getArrayMetadata()");
	}
		 
	@Override
	public CollectionMetadata getCollectionMetadata() throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("BaseDef does not support getCollectionMetadata()");
	}

	@Override
	public String getColumn() {
		return columns.get(0).getName();
	}

	@Override
	public String getCustomStrategy() throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("BaseDef does not support getCustomStrategy()");
	}

	@Override
	public Boolean getDefaultFetchGroup() {
		return defaultFetchGroup;
	}

	@Override
	public ForeignKeyAction getDeleteAction() {
		if (fks.size()>0)
			return fks.get(0).getDeleteAction();
		else
			return null;
	}

	@Override
	public Boolean getDependent() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ElementMetadata getElementMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean getEmbedded() {
		return super.embeddedOnly;
	}

	@Override
	public EmbeddedMetadata getEmbeddedMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFieldType() {
		return null;
	}

	@Override
	public IndexMetadata getIndexMetadata() {
		if (nonUniqueIndexes.size()>0)
			return nonUniqueIndexes.get(0);
		else
			return null;
	}

	@Override
	public Boolean getIndexed() {
		return nonUniqueIndexes.size()>0 || uniqueIndexes.size()>0;
	}

	@Override
	public JoinMetadata getJoinMetadata() {
		if (joins.size()>0)
			return joins.get(0);
		else
			return null;
	}

	@Override
	public KeyMetadata getKeyMetadata() {
		return null;
	}

	@Override
	public String getLoadFetchGroup() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MapMetadata getMapMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getMappedBy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NullValue getNullValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OrderMetadata getOrderMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistenceModifier getPersistenceModifier() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getRecursionDepth() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getSequence() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean getSerialized() {
		return serialized;
	}

	@Override
	public Boolean getUnique() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UniqueMetadata getUniqueMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ValueMetadata getValueMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IdGeneratorStrategy getValueStrategy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayMetadata newArrayMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CollectionMetadata newCollectionMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ElementMetadata newElementMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EmbeddedMetadata newEmbeddedMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyMetadata newKeyMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MapMetadata newMapMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OrderMetadata newOrderMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ValueMetadata newValueMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setCacheable(boolean cacheable) {
		super.cacheable = cacheable;
		return this;
	}

	@Override
	public MemberMetadata setColumn(String columnName) {
		if (columns.size()>0)
			columns.clear();
		columns.add(new ColumnDef(columnName, Types.NULL, 1));
		return this;
	}

	@Override
	public MemberMetadata setCustomStrategy(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setDefaultFetchGroup(boolean arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setDeleteAction(ForeignKeyAction arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setDependent(boolean arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setEmbedded(boolean arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setFieldType(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setIndexed(boolean arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setLoadFetchGroup(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setMappedBy(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setName(String name) {
		super.name = name;
		return this;
	}

	@Override
	public MemberMetadata setNullValue(NullValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setPersistenceModifier(PersistenceModifier arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setPrimaryKey(boolean arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setRecursionDepth(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setSequence(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setSerialized(boolean arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setTable(String table) {
		super.table = table;
		return this;
	}

	@Override
	public MemberMetadata setUnique(boolean arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MemberMetadata setValueStrategy(IdGeneratorStrategy arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ForeignKeyMetadata getForeignKeyMetadata() {
		if (fks.size()>0)
			return fks.get(0);
		else
			return null;
	}

	@Override
	public boolean getPrimaryKey() {
		// TODO Auto-generated method stub
		return false;
	}

}
