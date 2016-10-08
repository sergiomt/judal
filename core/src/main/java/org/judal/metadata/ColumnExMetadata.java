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

import javax.jdo.metadata.ExtensionMetadata;

/**
 * JDO ExtensionMetadata interface implementation
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class ColumnExMetadata implements ExtensionMetadata {

	public String key, value, vendor;
	
	public ColumnExMetadata(String key, String value, String vendor) {
		this.key = key;
		this.value = value;
		this.vendor = vendor;
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public String getVendorName() {
		return vendor;
	}

}
