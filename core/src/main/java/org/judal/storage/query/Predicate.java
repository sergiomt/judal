package org.judal.storage.query;

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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;

import javax.jdo.JDOUserException;

import com.knowgate.debug.DebugFile;

import static org.judal.storage.query.Connective.*;

/**
* Base class for Query Predicates
* Example of Usage #1: Get all contacts whose name begin with "A" and where created less than one day ago
* Date dNow = new Date();
* Date dOneDayAgo = new Date(dNow.getTime()-86400000l);
* Term t1 = new Term("tx_name","LIKE","A%");
* Term t2 = new Term("dt_created","BETWEEN",new Date[]{dNow,dOneDayAgo});
* AndPredicate p = new AndPredicate();
* p.add(t1);
* p.add(t2);
* DBSubset s = new DBSubset("k_contacts","*",p.getTextParametrized(), 100);
* s.load(JDCConnection, p.getParameters());
* Example of Usage #2: Get all contacts whose status is in a given list of values and do not have any opportunity
* Term t1 = new Term("id_status","IN",new String[]{"Active","JobChange","AnotherStatus"});
* Term t2 = new Term("SELECT gu_oportunity FROM k_oportunities o WHERE o.gu_contact=c.gu_contact","NOT EXISTS",null);
* AndPredicate p = new AndPredicate();
* p.add(t1);
* p.add(t2);
* DBSubset s = new DBSubset("k_contacts c","*",p.getTextParametrized(), 100);
* s.load(JDCConnection, p.getParameters());
* Example of Usage #3: Get all contacts whose title is "CEO" or "CIO" and are women or older that 45 years old
* Term t1 = new QueryTerm("de_title","IN",new String[]{"CEO","CIO"});
* Term t2 = new QueryTerm("id_gender","=","F");
* Term t3 = new QueryTerm("dt_birth","&lt;=",new Date()-1419120000000l);
* AndPredicate p = new AndPredicate();
* OrPredicate o = new OrPredicate();
* p.add(t1);
* o.add(t2);
* o.add(t3);
* p.add(o);
* DBSubset s = new DBSubset("k_contacts c","*",p.getTextParametrized(), 100);
* s.load(JDCConnection, p.getParameters());
* @author Sergio Montoro Ten
* @version 1.0
*
*/
public abstract class Predicate implements Cloneable, Part, Serializable {
	
	private static final long serialVersionUID = 1L;
	
	protected ArrayList<Part> oParts;
	
	private Connective oConnective;
	
	public Predicate() {
		oParts = new ArrayList<Part>();
		oConnective = NONE;
	}

	public abstract Predicate clone();
	
	protected void clone(Predicate source) {
		if (source.oParts==null)
			oParts = null;
		else
			oParts = new ArrayList<Part>(source.oParts.size());
			for (Part p: source.oParts)
				oParts.add(p.clone());
			oConnective = source.oConnective;
	}

	/**
	 * Add query part to predicate
	 * @param oPart QueryPartQueryPart
	 * @return this QueryPredicate
	 * @deprecated Use addPart
	 */
	@Deprecated
	public Predicate add (Part oPart) throws UnsupportedOperationException {
		return addPart(oPart);
	}

	/**
	 * Add query part to predicate
	 * @param oPart QueryPartQueryPart
	 * @return this QueryPredicate
	 */
	public Predicate addPart (Part oPart) throws UnsupportedOperationException {
		if (null==connective() || NONE.equals(connective()) )
			throw new UnsupportedOperationException("Cannot add part to a final predicate");
		oParts.add(oPart);
		return this;
	}
	
	/**
	 * Set connective to AND and add query part to predicate
	 * @param oPart QueryPartQueryPart
	 * @return this QueryPredicate
	 * @deprecated Use addPart
	 */
	@Deprecated
	public Predicate and (Part oPart) throws UnsupportedOperationException {
		return andPart(oPart);
	}

	/**
	 * Set connective to AND and add query part to predicate
	 * @param oPart QueryPartQueryPart
	 * @return this QueryPredicate
	 */
	public Predicate andPart (Part oPart) throws UnsupportedOperationException {
		if (oConnective.equals(NONE) || oConnective.equals(AND)) {
			oConnective = AND;
			oParts.add(oPart);
		} else {
			throw new UnsupportedOperationException("Cannot change connective from "+oConnective+" to "+AND);
		}
		return this;
	}
	
	/**
	 * Set connective to OR and add query part to predicate
	 * @param oPart QueryPartQueryPart
	 * @return this QueryPredicate
	 * @deprecated Use orPart
	 */
	@Deprecated
	public Predicate or (Part oPart) throws UnsupportedOperationException {
		return orPart(oPart);
	}

	/**
	 * Set connective to OR and add query part to predicate
	 * @param oPart QueryPartQueryPart
	 * @return this QueryPredicate
	 */
	public Predicate orPart (Part oPart) throws UnsupportedOperationException {
		if (oConnective.equals(NONE) || oConnective.equals(OR)) {
			oConnective = OR;
			oParts.add(oPart);
		} else {
			throw new UnsupportedOperationException("Cannot change connective from "+oConnective+" to "+OR);
		}
		return this;
	}
	
	/**
	 * Create a query part and add it to predicate
	 * @param constructorParameters Object... Parameters for the Part constructor
	 * @throws UnsupportedOperationException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @return this QueryPredicate
	 */
	public abstract Predicate add (Object... constructorParameters)
		throws	UnsupportedOperationException, NoSuchMethodException,
				SecurityException, InstantiationException, IllegalAccessException,
				IllegalArgumentException, InvocationTargetException;

	/**
	 * Set connective to AND and create a query part and add it to predicate
	 * @param constructorParameters Object... Parameters for the Part constructor
	 * @throws UnsupportedOperationException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @throws UnsupportedOperationException
	 * @return this QueryPredicate
	 */
	public Predicate and (Object... constructorParameters)
		throws	UnsupportedOperationException, NoSuchMethodException,
				SecurityException, InstantiationException, IllegalAccessException,
				IllegalArgumentException, InvocationTargetException, JDOUserException {
		if (oConnective.equals(NONE) || oConnective.equals(AND)) {
			oConnective = AND;
			return add(constructorParameters);
		} else {
			throw new UnsupportedOperationException("Cannot change connective from "+oConnective+" to "+AND);
		}
	}

	/**
	 * Set connective to OR and create a query part and add it to predicate
	 * @param constructorParameters Object... Parameters for the Part constructor
	 * @throws UnsupportedOperationException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @throws UnsupportedOperationException
	 * @return this QueryPredicate
	 */
	public Predicate or (Object... constructorParameters)
		throws	UnsupportedOperationException, NoSuchMethodException,
				SecurityException, InstantiationException, IllegalAccessException,
				IllegalArgumentException, InvocationTargetException {
		if (oConnective.equals(NONE) || oConnective.equals(OR)) {
			oConnective = OR;
			return add(constructorParameters);
		} else {
			throw new UnsupportedOperationException("Cannot change connective from "+oConnective+" to "+OR);
		}
	}
	
	/**
	 * Operator. Will be "AND" or "OR" depending on the derived class
	 * @return Connective
	 */
	public Connective connective() {
		return oConnective;
	}

	public int size() {
		return oParts.size();
	}

	/**
	 * Get predicate parts
	 * @return ArrayList&lt;Part&gt;
	 */
	public ArrayList<Part> parts() {
		return oParts;
	}

	/**
	 * Get array of parameter values
	 * return Object[]
	 */
	@Override
	public Object[] getParameters() {
		
		if (DebugFile.trace) {
			DebugFile.writeln("Begin Predicate.getParameters()");
			DebugFile.incIdent();
		}
		
		ArrayList<Object> oParams = new ArrayList<Object>();
		
		for (Part oPart : oParts) {
			if (oPart instanceof Term) {
				if (DebugFile.trace)
					DebugFile.writeln("getting parameters of nested Term");
				Term oTerm = (Term) oPart;
				if (!oTerm.getOperator().equals(Operator.IS) && !oTerm.getOperator().equals(Operator.ISNOT))
				  for (int v=0; v<oTerm.getValueCount(); v++)
					  if (null==oTerm.getValue(v))
						  oParams.add(oTerm.getValue(v));
					  else if (oTerm.getValue(v) instanceof Part)
						  oParams.addAll(Arrays.asList(((Part) oTerm.getValue(v)).getParameters()));
					  else
						  oParams.add(oTerm.getValue(v));
			} else {
				Object[] aParams = oPart.getParameters();
				if (DebugFile.trace)
					DebugFile.writeln("adding "+String.valueOf(aParams.length)+" parameters");
				for (int p=0; p<aParams.length; p++)
					if (aParams[p]==null)
						oParams.add(aParams[p]);
					else if (aParams[p] instanceof Part)
						oParams.addAll(Arrays.asList(((Part) aParams[p]).getParameters()));
					else
						oParams.add(aParams[p]);
			}
		} // next
		
		if (DebugFile.trace) {
			DebugFile.decIdent();
			if (oParams!=null && oParams.size()>0) {
				StringBuilder b = new StringBuilder();
				for (Object param : oParams)
					b.append(param==null ? null : param.getClass().getName()).append(",");
				if (b.length()>0) b.setLength(b.length()-1);
				DebugFile.writeln("End Predicate.getParameters() : { "+b.toString()+"}");
			} else {
				DebugFile.writeln("End Predicate.getParameters() : { }");
			}
		}
		
		return oParams.toArray();
	}

	/**
	 * <p>Get representation of this Predicate according to the query syntax required by the implementation.</p>
	 * @return Object
	 */
	public abstract Object getText();

	/**
	 * <p>Get parameterized text representation of this Predicate according to the query syntax required by the implementation.</p>
	 * @return String
	 */
	public abstract String getTextParametrized();
	
}