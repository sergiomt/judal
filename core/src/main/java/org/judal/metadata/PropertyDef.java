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

import javax.jdo.metadata.PropertyMetadata;

/**
 * JDO PropertyMetadata interface implementation
 * @author Sergio Montoro Ten
 *
 */
public class PropertyDef extends FieldDef implements PropertyMetadata {

	private String fieldName;

	public PropertyDef() {
	}

	public PropertyDef(PropertyDef source) {
		super(source);
		this.fieldName = source.fieldName;
	}

	@Override
	public PropertyDef clone() {
		return new PropertyDef(this);
	}
	
	@Override
	public String getFieldName() {
		return fieldName;
	}

	@Override
	public PropertyMetadata setFieldName(String fieldName) {
		this.fieldName = fieldName;
		return this;
	}

}
