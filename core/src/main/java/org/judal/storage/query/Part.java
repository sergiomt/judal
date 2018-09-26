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

/**
 * A predicate part. May be either a single term or a group of terms under QueryAndPredicate or QueryOrPredicate
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface Part extends Cloneable {

	Part clone();

	/**
	 * @return String or other suitable representation of the part in plain text including all the parameter values
	 */
	Object getText();

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
