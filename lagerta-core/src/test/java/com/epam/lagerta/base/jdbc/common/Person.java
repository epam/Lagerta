/*
 *  Copyright 2017 EPAM Systems.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.epam.lagerta.base.jdbc.common;

import java.io.Serializable;
import java.util.Objects;

public class Person implements Serializable {
    public static final String PERSON_CACHE = "person";
    public static final String PERSON_TABLE = "cache_information";
    public static final String PERSON_VAL = "val";
    public static final String PERSON_KEY = "key";
    public static final String PERSON_ID = "id";
    public static final String PERSON_NAME = "name";

    public static final int PERSON_ID_INDEX = 1;
    public static final int PERSON_KEY_INDEX = 2;
    public static final int PERSON_VAL_INDEX = 3;
    public static final int PERSON_NAME_INDEX = 4;

    private int id;
    private String name;

    public Person(int id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * This use for recreate object from map parameters.
     * see {@link com.epam.lagerta.util.JDBCKeyValueMapper#getPOJOFromMapParams(java.util.Map, java.lang.Class)}
     */
    @SuppressWarnings("unused")
    public Person() {
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Person)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        Person other = (Person) obj;
        return Objects.equals(id, other.id) && Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }
}
