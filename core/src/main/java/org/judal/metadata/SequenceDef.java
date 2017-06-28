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

import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.annotations.SequenceStrategy;
import javax.jdo.metadata.SequenceMetadata;

/**
 * JDO SequenceMetadata interface implementation
 * @author Sergio Montoro Ten
 *
 */
public class SequenceDef extends ExtendableDef implements SequenceMetadata {

	private static final long serialVersionUID = 10000l;

	private Integer initialValue;
	private Integer allocationSize;
	private String datastoreSequence;
	private String factoryClass;
	private String name;
	private SequenceStrategy strategy;
	
	/**
	 * @return Integer
	 */
	@Override
	public Integer getAllocationSize() {
		return allocationSize;
	}

	/**
	 * @return String
	 */
	@Override
	public String getDatastoreSequence() {
		return datastoreSequence;
	}

	/**
	 * @return String
	 */
	@Override
	public String getFactoryClass() {
		return factoryClass;
	}

	/**
	 * @return Integer
	 */
	@Override
	public Integer getInitialValue() {
		return initialValue;
	}

	/**
	 * @return String
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * @return SequenceStrategy
	 */
	@Override
	public SequenceStrategy getSequenceStrategy() {
		return strategy;
	}

	/**
	 * @param size int
	 * @return SequenceMetadata <b>this</b>
	 */
	@Override
	public SequenceMetadata setAllocationSize(int size) {
		allocationSize = new Integer(size);
		return this;
	}

	/**
	 * @param name String
	 * @return SequenceMetadata <b>this</b>
	 */
	@Override
	public SequenceMetadata setDatastoreSequence(String name) {
		datastoreSequence = name;
		return this;
	}

	/**
	 * @param className String Fully qualified class name
	 * @return SequenceMetadata <b>this</b>
	 */
	@Override
	public SequenceMetadata setFactoryClass(String className) {
		factoryClass = className;
		return this;
	}

	/**
	 * @param initial int
	 * @return SequenceMetadata <b>this</b>
	 */
	@Override
	public SequenceMetadata setInitialValue(int initial) {
		initialValue = new Integer(initial);
		return this;
	}

	/**
	 * <p>This method is not implemented and always throws a JDOUnsupportedOptionException.</p>
	 * @throws JDOUnsupportedOptionException
	 */
	public String source() throws JDOUnsupportedOptionException {
		throw new JDOUnsupportedOptionException("source() must be implemented by a subclass specific for a data source implementation");
	}
	
}
