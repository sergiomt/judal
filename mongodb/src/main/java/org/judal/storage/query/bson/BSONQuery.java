package org.judal.storage.query.bson;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.jdo.FetchGroup;
import javax.jdo.FetchPlan;
import javax.jdo.JDOException;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.TableDef;
import org.judal.metadata.ViewDef;
import org.judal.mongodb.MongoView;

import org.judal.storage.StorageObjectFactory;
import org.judal.storage.query.AbstractQuery;
import org.judal.storage.query.Connective;
import org.judal.storage.table.ColumnGroup;
import org.judal.storage.table.Record;
import org.judal.storage.table.RecordSet;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;

import com.knowgate.debug.DebugFile;

public class BSONQuery extends AbstractQuery {

	private static final int DEFAULT_INITIAL_RECORDSET_SIZE = 100;

	private static final long serialVersionUID = 1L;

	private Constructor<? extends Record> recordConstructor;
	private Object[] constructorParameters;
	private ViewDef tdef;
	private List<BsonDocument> pipeline;

	public BSONQuery(MongoView view, List<BsonDocument> pipeline) throws JDOException {
		if (view.getResultClass()==null)
			throw new NullPointerException("SQLQuery() IndexableView.getResultClass() cannot be null");
		setCandidates(view);
		setRange(0, Integer.MAX_VALUE);
		this.recordConstructor = null;
		this.tdef = view.getViewDef();
		this.pipeline = pipeline;
		setResultClass(view.getResultClass(), String.class);
	}

	@Override
	public BSONQuery clone() {
		BSONQuery theClone = new BSONQuery(getView(), pipeline);
		theClone.clone(this);
		if (null==pipeline) {
			theClone.pipeline = null;
		} else {
			theClone.pipeline = new ArrayList<>(pipeline.size());
			for (BsonDocument bdoc : pipeline)
				theClone.pipeline.add(bdoc);
		}
		return theClone;
	}

	private MongoView getView() {
		return (MongoView) getCandidates();
	}
	

	@Override
	public void setSerializeRead(Boolean serialize) {
		super.setSerializeRead(serialize);
	}

	@Override
	public String getResult() {
		final String retval = super.getResult();
		return retval==null ? "*" : retval;
	}

	@Override
	public Record newRecord() {
		return newRecord(new Document());
	}

	@SuppressWarnings("unchecked")
	public <R extends Record> R newRecord(Document doc) {
		if (null==recordConstructor) {
			recordConstructor = (Constructor<? extends Record>) StorageObjectFactory.getConstructor(getResultClass(), new Class<?>[]{TableDef.class, Document.class});
		}
		try {
			constructorParameters = StorageObjectFactory.filterParameters(recordConstructor.getParameters(), new Object[]{tdef,doc});
		} catch (InstantiationException e) {
			throw new JDOException(e.getMessage() + " getting constructor for Mongo Record", e);
		}
		return (R) StorageObjectFactory.newRecord(recordConstructor, constructorParameters);
	}

	@Override
	public BSONPredicate newPredicate(Connective logicalConnective) {
		switch(logicalConnective) {
		case AND:
			return new BSONAndPredicate();
		case OR:
			return new BSONOrPredicate();
		default:
			return new BSONPredicate();
		}
	}


	/**
	 * <p>Get filter Predicate.</p>
	 * @return BSONPredicate
	 */
	public BSONPredicate getFilterPredicate() {
		return (BSONPredicate) super.getFilterPredicate();
	}

	@Override
	public BSONPredicate newPredicate() throws JDOException {
		return newPredicate(Connective.NONE);
	}

	@SuppressWarnings("unchecked")
	@Override
	public RecordSet<? extends Record> execute() {
		RecordSet<? extends Record> retval;

		if (DebugFile.trace)
			DebugFile.writeln("Begin BSONQuery.execute(" + getRangeFromIncl() + "," + getRangeToExcl() + ")");

		if (getRangeFromIncl()<0l)
			throw new IllegalArgumentException("row offset must be equal to or greater than zero");

		if (DebugFile.trace)
			DebugFile.incIdent();

		try {

			retval = StorageObjectFactory.newRecordSetOf(getResultClass(), DEFAULT_INITIAL_RECORDSET_SIZE);

			if (null==pipeline) {

				FindIterable<Document> findIter = getView().getCollection().find(getFilterPredicate().getText());

				if (getResult()!=null && getResult().length()>0 && !getResult().equals("*")) {
					Document fields = new Document();
					for (String fieldName : getResult().split(","))
						fields.append(fieldName, true);
					findIter = findIter.projection(fields);
				}

				if (getRangeFromIncl()>0)
					findIter = findIter.skip(getRangeFromIncl().intValue());
				if (getRangeToExcl()>=getRangeFromIncl())
					findIter = findIter.limit((int) (getRangeToExcl().longValue()-getRangeFromIncl().longValue()));

				findIter.forEach((Block<Document>) doc -> retval.add(newRecord(doc)));

			} else {
				Bson bsonText = getFilterPredicate().getText();
				BsonDocument filter = bsonText.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry());
				getView().getCollection().aggregate(queryPipeline(filter)).forEach((Block<Document>) doc -> retval.add(newRecord(doc)));

			}
		}
		catch (MongoException | NoSuchMethodException e) {
			if (DebugFile.trace) DebugFile.writeln("MongoException "+e.getMessage());
			throw new JDOException(e.getClass().getName()+" "+e.getMessage(), e);
		}

		if (DebugFile.trace)
		{
			DebugFile.decIdent();
			DebugFile.writeln("End MongoQuery.execute() : "+String.valueOf(retval.size()));
		}

		return retval;
	} // execute

	@Override
	public Bson source() throws JDOException {
		return (Bson) getFilterPredicate();
	};

	/**
	 * <p>This function will always return <b>null</b>
	 */
	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	private BsonDocument projectFields(FetchGroup fetchGroup) {
		BsonDocument projFields = new BsonDocument();
		for (Object memberName : fetchGroup.getMembers()) {
			String fieldName = (String) memberName;
			if (fieldName.length()>0) {
				final String family = tdef.getColumnByName(fieldName).getFamily();
				if (family.equals(tdef.getName()))
					projFields.append(fieldName, new BsonBoolean(true));
				else
					projFields.append(fieldName, new BsonString("$"+family+"."+fieldName));
			}
		}
		BsonDocument project = new BsonDocument("$project", projFields);
		return project;
	}

	private List<BsonDocument> queryPipeline(BsonDocument filter) {
		BsonDocument project = projectFields(new ColumnGroup(tdef.getColumnsStr().split(",")));
		BsonDocument match = new BsonDocument("$match", filter);
		return Arrays.asList(pipeline.get(0), pipeline.get(1), project, match);
	}

}
