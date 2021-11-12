package org.judal.storage;

/*
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

public class Pair<T,S> implements Serializable {

    private static final long serialVersionUID = 8L;

    private T val1;
    private S val2;

    public Pair(T val1, S val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    public T $1() {
        return val1;
    }

    public S $2() {
        return val2;
    }

    public T getName() {
        return val1;
    }

    public S getValue() {
        return val2;
    }

    public static Pair<String,String> P$(String val1, String val2) {
        return new Pair<String,String>(val1, val2);
    }

    public static Pair<String,Object> P_(String val1, Object val2) {
        return new Pair<String,Object>(val1, val2);
    }
}
