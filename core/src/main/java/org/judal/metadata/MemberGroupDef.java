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

import java.util.ArrayList;

import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.metadata.FetchGroupMetadata;
import javax.jdo.metadata.FieldMetadata;
import javax.jdo.metadata.MemberMetadata;
import javax.jdo.metadata.PropertyMetadata;

/**
* JDO FetchGroupMetadata interface implementation 
* @author Sergio Montoro Ten
* @version 1.0
*/
public class MemberGroupDef extends ExtendableDef implements FetchGroupMetadata {

	private static final long serialVersionUID = 10000l;

	private String name;
	private boolean postLoad;
	private ArrayList<FieldMetadata> members;
	
	/**
	 * Constructor
	 * @param groupName String
	 * @param fieldNames String&hellip;
	 */
	public MemberGroupDef(String groupName, String... fieldNames) {
		name = groupName;
		if (fieldNames==null) {
			members = new ArrayList<FieldMetadata>();			
		} else {
			members = new ArrayList<FieldMetadata>(fieldNames.length);
			for (String fieldName : fieldNames)
				newFieldMetadata(fieldName);			
		}
	}

	/**
	 * @return String
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * @param name String
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * @return MemberMetadata[]
	 */
	@Override
	public MemberMetadata[] getMembers() {
		return members.toArray(new FieldMetadata[members.size()]);
	}

	/**
	 * <p>Get number of members.</p>
	 * @return int
	 */
	@Override
	public int getNumberOfMembers() {
		return members.size();
	}

	/**
	 * @return boolean
	 */
	@Override
	public Boolean getPostLoad() {
		return postLoad;
	}

	/**
	 * <p>Create a new FieldDef and append it to the members of this MemberGroupDef.</p>
	 * @param fieldName String
	 * @return FieldDef
	 */
	@Override
	public FieldDef newFieldMetadata(String fieldName) {
		FieldDef fieldMeta = new FieldDef();
		fieldMeta.setName(fieldName);
		members.add(fieldMeta);
		return fieldMeta;
	}

	/**
	 * <p>This method is not implemented and always throws JDOUnsupportedOptionException.</p>
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public PropertyMetadata newPropertyMetadata(String arg0) {
		throw new JDOUnsupportedOptionException("newPropertyMetadata");
	}

	/**
	 * @param postLoad boolean
	 * @return MemberGroupDef
	 */
	@Override
	public MemberGroupDef setPostLoad(boolean postLoad) {
		this.postLoad = postLoad;
		return this;
	}

}
