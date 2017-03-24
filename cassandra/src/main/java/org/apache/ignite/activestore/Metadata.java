/*
 * Copyright (c) 2017. EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.activestore;

import java.io.Serializable;

/**
 * Abstraction for one snapshot in persistence storage.
 */
public class Metadata implements Comparable<Metadata>, Serializable {
    /**
     * Time of creation.
     */
    private final long timestamp;

    /**
     * Creates metadata with given settings.
     *
     * @param timestamp time of creation.
     */
    public Metadata(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Returns time of creation.
     *
     * @return time.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns label which is created by default for any metadata by system.
     *
     * @return default label.
     */
    public String getDefaultLabel() {
        return String.valueOf(timestamp);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Metadata metadata = (Metadata)o;
        return timestamp == metadata.timestamp;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(timestamp ^ (timestamp >>> 32));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Metadata{" +
            "timestamp=" + timestamp +
            '}';
    }

    /** {@inheritDoc} */
    @Override public int compareTo(Metadata o) {
        return Long.compare(timestamp, o.timestamp);
    }
}
