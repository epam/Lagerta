--
-- Copyright (c) 2017. EPAM Systems
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--   limitations under the License.
--

CREATE TABLE primitivesTable (
    key INT PRIMARY KEY,
    val BLOB,
    booleanValue BIT,
    byteValue TINYINT,
    shortValue SMALLINT,
    intValue INT,
    longValue BIGINT,
    floatValue REAL,
    doubleValue FLOAT
);

CREATE TABLE primitiveWrappersTable (
      key INT PRIMARY KEY,
      val BLOB,
      booleanValue BIT,
      byteValue TINYINT,
      shortValue SMALLINT,
      intValue INT,
      longValue BIGINT,
      floatValue REAL,
      doubleValue FLOAT
);

CREATE TABLE otherTypesTable (
      key INT PRIMARY KEY,
      val BLOB,
      bytesValue BINARY,
      bigDecimalValue DECIMAL,
      dateValue DATE,
      timestampValue TIMESTAMP
);
