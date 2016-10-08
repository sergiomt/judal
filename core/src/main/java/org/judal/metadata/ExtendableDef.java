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

import java.util.Arrays;

import javax.jdo.metadata.Metadata;
import javax.jdo.metadata.ExtensionMetadata;

/**
 * JDO Metadata interface implementation
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public abstract class ExtendableDef implements Metadata {

	private Metadata parent;
	private ExtensionMetadata[] extMetadata;
	
	protected ExtendableDef() {
		parent = null;
		extMetadata = null;
	}

	protected ExtendableDef(ExtendableDef source) {
		parent = source.parent;
		extMetadata = source.extMetadata;
	}

	/**
	 * @return ExtensionMetadata[] Reference to the array of ExtensionMetadata
	 */
	@Override
	public ExtensionMetadata[] getExtensions() {
		if (null==extMetadata)
			extMetadata = new ExtensionMetadata[0];
		return extMetadata;
	}

	/**
	 * @return int
	 */
	@Override
	public int getNumberOfExtensions() {
		return getExtensions().length;
	}

	@Override
	public Metadata getParent() {
		return parent;
	}

	protected void setParent(Metadata parent) {
		this.parent = parent;
	}
	
	/**
	 * Create a new ExtensionMetadata instance and append it to this object
	 * @param sKey String Extension Key/Name
	 * @param sValue String Extension Value
	 * @param sVendor String Extension Vendor Name
	 * @return ExtensionMetadata <b>this</b> object
	 */
	@Override
	public ExtensionMetadata newExtensionMetadata(String sKey, String sValue, String sVendor) {
		ColumnExMetadata oColExMetadata = new ColumnExMetadata(sKey,sValue,sVendor);
		if (null==extMetadata)
			extMetadata = new ExtensionMetadata[]{oColExMetadata};
		else if (extMetadata.length==0)
			extMetadata = new ExtensionMetadata[]{oColExMetadata};
		else {
			int insertAt = extMetadata.length;
			extMetadata = Arrays.copyOf(extMetadata, insertAt+1);
			extMetadata[insertAt] = oColExMetadata;
		}
		return oColExMetadata;
	}

}
