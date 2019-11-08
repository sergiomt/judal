package org.judal.mongodb;

/**
 * © Copyright 2018 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.Decoder;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;
import org.judal.metadata.ViewDef;
import org.judal.storage.Param;
import org.judal.storage.StorageObjectFactory;
import org.judal.storage.keyvalue.Stored;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Predicate;
import org.judal.storage.query.bson.BSONConverter;
import org.judal.storage.query.bson.BSONPredicate;
import org.judal.storage.query.bson.BSONQuery;
import org.judal.storage.relational.RelationalView;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.judal.storage.table.SchemalessIndexableView;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.connection.Cluster;
import com.mongodb.operation.AggregateOperation;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.ReadOperation;

public class MongoView extends MongoBucket implements RelationalView, SchemalessIndexableView {

	private static final int DEFAULT_INITIAL_RECORDSET_SIZE = 100;

	protected ViewDef tableDef;
	protected Class<? extends Stored> candidateClass;
	private Class<? extends Record> recordClass;
	private Constructor<? extends Record> recordConstructor;
	private List<BsonDocument> pipeline;

	public MongoView(Cluster cluster, String databaseName, TableDef tableDef, MongoCollection<Document> collection, Class<? extends Record> recClass) throws JDOException {
		super(cluster, databaseName, tableDef.getName(), collection);
		this.tableDef = tableDef;
		this.candidateClass = this.recordClass = recClass;
		this.recordConstructor = null;
		this.pipeline = null;
	}

	public MongoView(Cluster cluster, String databaseName, String tableName, MongoCollection<Document> collection, Class<? extends Record> recClass) throws JDOException {
		super(cluster, databaseName, tableName, collection);
		this.tableDef = null;
		this.candidateClass = this.recordClass = recClass;
		this.recordConstructor = null;
		this.pipeline = null;
	}

	public MongoView(Cluster cluster, String databaseName, TableDef tableDef, MongoCollection<Document> collection, Class<? extends Record> recClass, List<BsonDocument> pipeline) throws JDOException {
		this(cluster, databaseName, tableDef, collection, recClass);
		this.pipeline = pipeline;
	}

	@Override
	public ColumnDef[] columns() {
		return tableDef.getColumns();
	}

	@Override
	public int columnsCount() {
		return tableDef.getNumberOfColumns();
	}

	@Override
	public ColumnDef getColumnByName(String columnName) {
		return tableDef.getColumnByName(columnName);
	}

	@Override
	public int getColumnIndex(String columnName) {
		int position = 1;
		for (ColumnDef c :columns())
			if (c.getName().equalsIgnoreCase(columnName))
				return position;
			else
				position++;
		return -1;
	}

	private BsonDocument projectFields(FetchGroup fetchGroup) {
		BsonDocument projFields = new BsonDocument();
		for (Object fieldName : fetchGroup.getMembers()) {
			final String family = getColumnByName((String) fieldName).getFamily();
			if (family.equals(tableDef.getName()))
				projFields.append((String) fieldName, new BsonBoolean(true));
			else
				projFields.append((String) fieldName, new BsonString("$"+family+"."+fieldName));
		}
		BsonDocument project = new BsonDocument();
		project.put("$project", projFields);
		return project;
	}

	private List<BsonDocument> projectPipeline(FetchGroup fetchGroup) {
		BsonDocument project = projectFields(fetchGroup);
		return Arrays.asList(pipeline.get(0), pipeline.get(1), project);
	}

	private List<BsonDocument> countPipeline(String indexColumnName, Object valueSearched) {
		BsonDocument count = new BsonDocument();
		if (null==indexColumnName) {
			count.append("$count", new BsonString("rowsCount"));
			return Arrays.asList(pipeline.get(0), pipeline.get(1), count);
		} else {
			BsonDocument project = projectFields(new ColumnGroup(indexColumnName));
			count.append("$count", new BsonString("rowsCount"));
			BsonDocument match = new BsonDocument();
			match.put("$match", new BsonDocument(indexColumnName, BSONConverter.convert(valueSearched)));
			final String family = getColumnByName((String) indexColumnName).getFamily();
			if (family.equals(tableDef.getName()))
				return Arrays.asList(match, pipeline.get(0), pipeline.get(1), project, count);
			else
				return Arrays.asList(pipeline.get(0), pipeline.get(1), project, match, count);
		}
	}

	private List<BsonDocument> countPipeline(BsonDocument filter) {
		BsonDocument project = projectFields(new ColumnGroup(tableDef.getColumnsStr().split(",")));
		BsonDocument count = new BsonDocument("$count", new BsonString("rowsCount"));
		if (filter!=null)
			return Arrays.asList(pipeline.get(0), pipeline.get(1), project, new BsonDocument("$match", filter), count);
		else
			return Arrays.asList(pipeline.get(0), pipeline.get(1), project, count);
	}

	private List<BsonDocument> aggregatePipeline(BsonDocument filter, String aggFunc, String fieldName) {
		BsonDocument project = projectFields(new ColumnGroup(tableDef.getColumnsStr().split(",")));
		BsonDocument group = new BsonDocument();
		group.append("_id", new BsonString("exchange"));
		group.append(aggFunc+"_"+fieldName, new BsonDocument("$"+aggFunc, new BsonString("$"+fieldName)));
		BsonDocument aggregate = new BsonDocument("$group", group);
		if (filter!=null)
			return Arrays.asList(pipeline.get(0), pipeline.get(1), project, new BsonDocument("$match", filter), aggregate);
		else
			return Arrays.asList(pipeline.get(0), pipeline.get(1), project, aggregate);
	}

	private List<BsonDocument> existsPipeline(Param... keys) {
		if (keys==null || keys.length<1) {
			throw new JDOException("Parameters list cannot be empty");
		} else {
			ColumnGroup cols = new ColumnGroup();
			for (Param p : keys)
				cols.addMember(p.getName());
			BsonDocument project = projectFields(cols);
			BsonDocument limit = new BsonDocument("$limit", new BsonInt32(1));
			BsonDocument match = new BsonDocument();
			if (keys.length==1) {
				match.put("$match", new BsonDocument(keys[0].getName(), BSONConverter.convert(keys[0].getValue())));
			} else {
				BsonArray matches = new BsonArray();
				for (Param p : keys)
					matches.add(new BsonDocument(p.getName(), BSONConverter.convert(p.getValue())));
				match.put("$match", new BsonDocument("$and", matches));
			}
			return Arrays.asList(pipeline.get(0), pipeline.get(1), project, match, limit);
		}
	}

	private List<BsonDocument> projectPipeline(FetchGroup fetchGroup, String fieldName, Object fieldValue) {
		BsonDocument namevalue = new BsonDocument();
		namevalue.append(fieldName, BSONConverter.convert(fieldValue));
		BsonDocument match = new BsonDocument();
		match.put("$match", namevalue);
		BsonDocument project = projectFields(fetchGroup);
		return Arrays.asList(match, pipeline.get(0), pipeline.get(1), project);
	}

	private List<BsonDocument> projectPipeline(FetchGroup fetchGroup, String fieldName, Object fieldValue, int maxrows, int offset) {
		BsonDocument namevalue = new BsonDocument(fieldName, BSONConverter.convert(fieldValue));
		BsonDocument match = new BsonDocument("$match", namevalue);
		BsonDocument project = projectFields(fetchGroup);
		BsonDocument skip = new BsonDocument("$skip", new BsonInt32(offset));
		BsonDocument limit = new BsonDocument("$limit", new BsonInt32(maxrows));
		return Arrays.asList(match, pipeline.get(0), pipeline.get(1), skip, limit, project);
	}

	private List<BsonDocument> projectPipeline(FetchGroup fetchGroup, String fieldName, Comparable<?> valueFrom, Comparable<?> valueTo) {
		BsonDocument range = new BsonDocument();
		range.append("$gte", BSONConverter.convert(valueFrom));
		range.append("$lte", BSONConverter.convert(valueTo));
		BsonDocument namerange = new BsonDocument(fieldName, range);
		BsonDocument match = new BsonDocument("$match", namerange);
		BsonDocument project = projectFields(fetchGroup);
		return Arrays.asList(match, pipeline.get(0), pipeline.get(1), project);
	}

	private List<BsonDocument> projectPipeline(FetchGroup fetchGroup, String fieldName, Comparable<?> valueFrom, Comparable<?> valueTo, int maxrows, int offset) {
		BsonDocument range = new BsonDocument();
		range.append("$gte", BSONConverter.convert(valueFrom));
		range.append("$lte", BSONConverter.convert(valueTo));
		BsonDocument namerange = new BsonDocument(fieldName, range);
		BsonDocument match = new BsonDocument("$match", namerange);
		BsonDocument project = projectFields(fetchGroup);
		BsonDocument skip = new BsonDocument("$skip", new BsonInt32(offset));
		BsonDocument limit = new BsonDocument("$limit", new BsonInt32(maxrows));
		return Arrays.asList(match, pipeline.get(0), pipeline.get(1), skip, limit, project);
	}

	private List<BsonDocument> projectPipeline(FetchGroup fetchGroup, Param[] params, int maxrows, int offset) {
		BsonArray conditions = new BsonArray();
		for (Param p : params)
			conditions.add(new BsonDocument(p.getName(), BSONConverter.convert(p.getValue())));
		BsonDocument match = new BsonDocument("$match", new BsonDocument("$and", conditions));
		BsonDocument project = projectFields(fetchGroup);
		BsonDocument skip = new BsonDocument("$skip", new BsonInt32(offset));
		BsonDocument limit = new BsonDocument("$limit", new BsonInt32(maxrows));
		return Arrays.asList(match, pipeline.get(0), pipeline.get(1), skip, limit, project);
	}

	private Document fetchFields(FetchGroup fetchGroup) {
		Document fields = new Document();
		for (Object fieldName : fetchGroup.getMembers())
			fields.append((String) fieldName, true);
		return fields;
	}

	@Override
	public BatchCursor<Document> getCursor() {
		ReadPreference readPref = ReadPreference.primary();
		ReadConcern concern = ReadConcern.DEFAULT;
		MongoNamespace ns = new MongoNamespace(getDatabase(),name());
		Decoder<Document> codec = new DocumentCodec();
		ReadWriteBinding readBinding = new ClusterBinding(getCluster(), readPref, concern);
		ReadOperation<BatchCursor<Document>> fop;
		if (pipeline==null) {
			fop = new FindOperation<Document>(ns,codec);
		} else {
			ColumnGroup fetchGroup = new ColumnGroup();
			for (ColumnDef cdef : tableDef.getColumns())
				fetchGroup.addMember(cdef.getName());
			fop = new AggregateOperation<Document>(ns, projectPipeline(fetchGroup), codec);
		}
		return fop.execute(readBinding);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Iterator iterator() {
		return new MongoIterator(tableDef, getCursor());
	}

	@Override
	public boolean exists(Object key) throws JDOException {
		if (key instanceof Param)
			return exists(new Param[] {(Param) key});
		else
			return super.exists(key);
	}

	@Override
	public boolean exists(Param... keys) throws JDOException {
		Bson[] filters = new Bson[keys.length];
		int n = 0;
		for (Param p : keys)
			filters[n++] = Filters.eq(p.getName(), p.getValue());
		try {
			if (null==pipeline) {
				return getCollection().find(Filters.and(filters)).limit(1).first()!=null;
			} else {
				return getCollection().aggregate(existsPipeline(keys)).first()!=null;
			}
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		}
	}

	@Override
	public long count(String indexColumnName, Object valueSearched) throws JDOException {
		try {
			if (null==pipeline) {
				if (indexColumnName==null)
					return getCollection().countDocuments();
				else
					return getCollection().countDocuments(Filters.eq(indexColumnName, valueSearched));
			} else {
				Document counter = getCollection().aggregate(countPipeline(indexColumnName, valueSearched)).first();
				return counter==null ? 0 : counter.getInteger("rowsCount").longValue();
			}
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" MongoView.count("+indexColumnName+","+valueSearched+") " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched) throws JDOException {
		Bson filter = Filters.eq(indexColumnName, valueSearched);
		try {
			final RecordSet<R> recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recordClass, (int) getCollection().countDocuments(filter));
			if (null==pipeline)
				getCollection().find(filter).projection(fetchFields(fetchGroup)).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			else
				getCollection().aggregate(projectPipeline(fetchGroup,indexColumnName, valueSearched)).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			return recordSet;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new JDOException("NoSuchMethodException " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Object valueSearched, int maxrows, int offset) throws JDOException {
		Bson filter = Filters.eq(indexColumnName, valueSearched);
		Document fields = new Document();
		for (Object fieldName : fetchGroup.getMembers())
			fields.append((String) fieldName, true);
		try {
			int count = DEFAULT_INITIAL_RECORDSET_SIZE - offset;
			if (count<0)
				count = DEFAULT_INITIAL_RECORDSET_SIZE;
			if (maxrows>0 && count>maxrows)
				count = maxrows;
			final RecordSet<R> recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recordClass, count);
			if (null==pipeline)
				getCollection().find(filter).limit(maxrows).skip(offset).projection(fields).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			else
				getCollection().aggregate(projectPipeline(fetchGroup,indexColumnName, valueSearched, maxrows, offset)).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			return recordSet;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new JDOException("NoSuchMethodException " + e.getMessage(), e);
		}	
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo) throws JDOException, IllegalArgumentException {
		Bson from = Filters.gte(indexColumnName, valueFrom);
		Bson to = Filters.lte(indexColumnName, valueTo);
		Bson range = Filters.and(from,to);
		Document fields = new Document();
		for (Object fieldName : fetchGroup.getMembers())
			fields.append((String) fieldName, true);
		try {
			final RecordSet<R> recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recordClass, (int) getCollection().countDocuments(range));
			if (null==pipeline)
				getCollection().find(range).projection(fields).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			else
				getCollection().aggregate(projectPipeline(fetchGroup,indexColumnName, valueFrom, valueTo)).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));				
			return recordSet;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new JDOException("NoSuchMethodException " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, String indexColumnName, Comparable<?> valueFrom, Comparable<?> valueTo, int maxrows, int offset) throws JDOException, IllegalArgumentException {
		Bson from = Filters.gte(indexColumnName, valueFrom);
		Bson to = Filters.lte(indexColumnName, valueTo);
		Bson range = Filters.and(from,to);
		Document fields = new Document();
		for (Object fieldName : fetchGroup.getMembers())
			fields.append((String) fieldName, true);
		int count = DEFAULT_INITIAL_RECORDSET_SIZE - offset;
		if (count<0)
			count = DEFAULT_INITIAL_RECORDSET_SIZE;
		if (maxrows>0 && count>maxrows)
			count = maxrows;
		try {
			final RecordSet<R> recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recordClass, count);
			if (null==pipeline)
				getCollection().find(range).limit(maxrows).skip(offset).projection(fields).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			else
				getCollection().aggregate(projectPipeline(fetchGroup,indexColumnName, valueFrom, valueTo,maxrows, offset)).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));				
			return recordSet;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new JDOException("NoSuchMethodException " + e.getMessage(), e);
		}
	}

	@Override
	public Class<? extends Record> getResultClass() {
		return recordClass;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(FetchGroup fetchGroup, int maxrows, int offset, Param... params) {
		Bson[] filters = new Bson[params.length];
		int f = 0;
		for (Param p : params)
			filters[f++] = Filters.eq(p.getName(), p.getValue());
		Bson filter = Filters.and(filters);
		Document fields = new Document();
		for (Object fieldName : fetchGroup.getMembers())
			fields.append((String) fieldName, true);
		int count = DEFAULT_INITIAL_RECORDSET_SIZE - offset;
		if (count < 0)
			count = DEFAULT_INITIAL_RECORDSET_SIZE;
		if (maxrows > 0 && count > maxrows)
			count = maxrows;
		try {
			final RecordSet<R> recordSet = (RecordSet<R>) StorageObjectFactory.newRecordSetOf(recordClass, count);
			if (null==pipeline)
				getCollection().find(filter).limit(maxrows).skip(offset).projection(fields).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			else
				getCollection().aggregate(projectPipeline(fetchGroup, params, maxrows, offset)).forEach((Block<Document>) doc -> recordSet.add((R) newRecord(doc)));
			return recordSet;
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new JDOException("NoSuchMethodException " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	public Record newRecord(Document bsonDoc) {
		Object[] constructorParameters;
		if (null==recordConstructor) {
			recordConstructor = (Constructor<? extends Record>) StorageObjectFactory.getConstructor(getResultClass(), new Class<?>[]{TableDef.class, Document.class});
		}
		try {
			constructorParameters = StorageObjectFactory.filterParameters(recordConstructor.getParameters(), new Object[]{tableDef, bsonDoc});
		} catch (InstantiationException e) {
			throw new JDOException(e.getMessage() + " getting constructor for MongoDocument subclass");
		}
		return StorageObjectFactory.newRecord(recordConstructor, constructorParameters);
	}

	@Override
	public BSONQuery newQuery() throws JDOException {
		return new BSONQuery(this, pipeline);
	}

	@Override
	public BSONPredicate newPredicate() {
		return new BSONPredicate();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> RecordSet<R> fetch(AbstractQuery query) throws JDOException {
		return (RecordSet<R>) query.execute();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R extends Record> R fetchFirst(AbstractQuery query) throws JDOException {
		if (query.getFilterPredicate()==null)
			throw new NullPointerException("Query predicate cannot be null");
		AbstractQuery query1;
		if (query.getMaxRows()==1) {
			query1  = query;
		} else {
			query1 = query.clone();
			if (query1.getFilterPredicate()==null)
				throw new NullPointerException("Query clone predicate cannot be null");
			if (query1.getRangeToExcl()-query1.getRangeFromIncl()>1) {
				query1.setRange(query1.getRangeFromIncl(), query1.getRangeFromIncl()+1);
			}
		}
		RecordSet<R> rst = (RecordSet<R>) query1.execute();
		return rst.size()>0 ? rst.get(0) : null;
	}

	@Override
	public Long count(Predicate filterPredicate) throws JDOException {
		Bson filter = (Bson) filterPredicate.getText();
		try {
			if (null==pipeline) {
				return getCollection().countDocuments(filter);
			} else {
				BsonDocument match = filter.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry());
				Document counter = getCollection().aggregate(countPipeline(match)).first();
				return counter==null ? 0 : counter.getInteger("rowsCount").longValue();
			}
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		}
	}

	private Document aggregateFunction(String result, Predicate filterPredicate, String aggFunc) {
		try {
			AggregateIterable<Document> aggregate;
			BsonDocument filter = null;
			if (filterPredicate!=null)
				filter = ((Bson) filterPredicate.getText()).toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry());
			if (null==pipeline) {
				BsonDocument project = new BsonDocument("$project", new BsonDocument (aggFunc+"_"+result, new BsonDocument ("$"+aggFunc, new BsonString("$"+result))));
				if (filter!=null)
					aggregate = getCollection().aggregate(Arrays.asList(new BsonDocument("$match", filter), project));
				else
					aggregate = getCollection().aggregate(Arrays.asList(project));
			} else {
				aggregate = getCollection().aggregate(aggregatePipeline(filter, aggFunc, result));
			}
			return aggregate.first();
		} catch (MongoException e) {
			throw new JDOException(e.getClass().getName()+" " + e.getMessage(), e);
		}

	}

	private Object aggregateObjectFunction(String result, Predicate filterPredicate, String aggFunc) {
		Document doc = aggregateFunction(result, filterPredicate, aggFunc);
		return doc!=null ? doc.get(aggFunc+"_"+result) : null;
	}

	private Double aggregateDoubleFunction(String result, Predicate filterPredicate, String aggFunc) {
		Document doc = aggregateFunction(result, filterPredicate, aggFunc);
		return doc!=null ? doc.getDouble(aggFunc+"_"+result) : null;
	}

	@Override
	public Number avg(String result, Predicate filterPredicate) throws JDOException {
		return aggregateDoubleFunction(result, filterPredicate, "avg");
	}

	@Override
	public Number sum(String result, Predicate filterPredicate) throws JDOException {
		return aggregateDoubleFunction(result, filterPredicate, "sum");
	}

	@Override
	public Object max(String result, Predicate filterPredicate) throws JDOException {
		return aggregateObjectFunction(result, filterPredicate, "max");
	}

	@Override
	public Object min(String result, Predicate filterPredicate) throws JDOException {
		return aggregateObjectFunction(result, filterPredicate, "min");
	}

	public ViewDef getViewDef() {
		return tableDef;
	}
}
