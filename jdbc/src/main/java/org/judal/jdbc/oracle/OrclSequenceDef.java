package org.judal.jdbc.oracle;

/*
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

import org.judal.metadata.Scriptable;
import org.judal.metadata.SequenceDef;
import org.judal.storage.Param;

public class OrclSequenceDef extends SequenceDef implements Scriptable {

	private static final long serialVersionUID = 1L;

	@Override
	public String getSource() {
		return "CREATE SEQUENCE "+getName()+" INCREMENT BY 1 START WITH "+String.valueOf(getInitialValue());
	}

	@Override
	public Param[] getParams() {
		return new Param[0];
	}

	@Override
	public String getDrop() {
		return "DROP SEQUENCE "+getName();
	}

}
