package org.judal.storage.query.bson;

/**
 * © Copyright 2018 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import org.judal.storage.query.Connective;

public class BSONAndPredicate extends BSONPredicate {

	private static final long serialVersionUID = 1L;

	@Override
	public Connective connective() {
		return Connective.AND;
	}

}
