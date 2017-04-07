/*
 * Copyright (c) 2017. EPAM Systems.
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

package com.epam.lathgertha.subscriber.lead;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Heartbeats {
    private final long expirationThreshold;

    private final Map<UUID, Long> beats = new HashMap<>();

    public Heartbeats(long expirationThreshold) {
        this.expirationThreshold = expirationThreshold;
    }

    public boolean isAvailable(UUID consumerId) {
        Long beat = beats.get(consumerId);
        return beat != null && !isBeatExpired(beat);
    }

    public Iterable<UUID> knownReaders() {
        return beats.keySet();
    }

    private boolean isBeatExpired(long beat) {
        return System.currentTimeMillis() - beat > expirationThreshold;
    }

    public void update(UUID consumerId, long beat) {
        beats.put(consumerId, beat);
    }

    public void removeDead(UUID consumerId) {
        beats.remove(consumerId);
    }
}
