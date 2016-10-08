package org.judal.storage.query;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import javax.jdo.JDOUserException;

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
* Term t3 = new QueryTerm("dt_birth","<=",new Date()-1419120000000l);
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
public abstract class Predicate implements Part,Serializable {
	
	private static final long serialVersionUID = 1L;
	
	protected ArrayList<Part> oParts;
	
	private Connective oConnective;
	
	public Predicate() {
		oParts = new ArrayList<Part>();
		oConnective = NONE;
	}

	/**
	 * Add query part to predicate
	 * @param oPart QueryPartQueryPart
	 * @return this QueryPredicate
	 */
	public Predicate add (Part oPart) throws UnsupportedOperationException {
		if (null==connective() || NONE.equals(connective()) )
			throw new UnsupportedOperationException("Cannot add part to a final predicate");
		oParts.add(oPart);
		return this;
	}

	/**
	 * Set connective to AND and add query part to predicate
	 * @param oPart QueryPartQueryPart
	 * @return this QueryPredicate
	 */
	public Predicate and (Part oPart) throws UnsupportedOperationException {
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
	 */
	public Predicate or (Part oPart) throws UnsupportedOperationException {
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
	 * @return ArrayList<QueryPart>
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
		ArrayList<Object> oParams = new ArrayList<Object>();
		for (Part oPart : oParts) {
			if (oPart instanceof Term) {
				Term oTerm = (Term) oPart;
				if (!oTerm.getOperator().equals(Operator.IS) && !oTerm.getOperator().equals(Operator.ISNOT))
				  for (int v=0; v<oTerm.getValueCount(); v++)
					  oParams.add(oTerm.getValue(v));
			} else {
				Object[] aParams = oPart.getParameters();
				for (int p=0; p<aParams.length; p++)
					oParams.add(aParams[p]);
			}
		} // next
		return oParams.toArray();
	}

	public abstract String getText();

	public abstract String getTextParametrized();
	
}