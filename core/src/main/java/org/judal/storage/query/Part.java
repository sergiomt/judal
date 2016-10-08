package org.judal.storage.query;

/*
 * A predicate part. May be either a single term or a group of terms under QueryAndPredicate or QueryOrPredicate
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface Part {

	/**
	 * @return String The part in plain text including all the parameter values
	 */
	String getText();

	/**
	 * @return String The part with its parameters as question marks "?"
	 */
	String getTextParametrized();
	
	/**
	 * @return Object[] Parameter values
	 */
	Object[] getParameters();	

	/**
	 * @return Count of subparts that this parts contains. It is 1 if the part is a QueryTerm or may be greater than one if the part is a QueryAndPredicate or a QueryOrPredicate
	 */
	public int size();
}
