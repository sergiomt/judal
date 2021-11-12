package org.judal.bdb;

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

import java.io.Serializable;

public class DBEntityWrapper implements Serializable {

	private static final long serialVersionUID = 1L;

	private byte[] key;
	private Serializable wrapped;

	public DBEntityWrapper(byte[] key, Serializable wrapped) {
		this.key = key;
		this.wrapped = wrapped;
	}

	public byte[] getKey() {
		return key;
	}

	public Serializable getWrapped() {
		return wrapped;
	}

}
