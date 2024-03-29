package org.judal.inmemory;

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

import java.util.Collection;

import org.judal.metadata.TableDef;
import org.judal.metadata.ColumnDef;

public class InMemoryTableDef extends TableDef {

	private static final long serialVersionUID = 1L;

	public InMemoryTableDef(String name) {
		super(name);
	}

	public InMemoryTableDef(String name, ColumnDef... columnDefs) {
		super(name, columnDefs);
	}

	public InMemoryTableDef(String name, Collection<ColumnDef> columnDefs) {
		super(name, columnDefs);
	}
}
