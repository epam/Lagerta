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
package com.epam.lathgertha.subscriber.util;

import com.epam.lathgertha.subscriber.ConsumerTxScope;
import com.epam.lathgertha.subscriber.lead.CommittedTransactions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

public final class PlannerUtil {
    private static final Function<UUID, Set<Map.Entry<String, List>>> CLAIMED = key -> new HashSet<>();
    private static final Function<UUID, List<Long>> READY = key -> new ArrayList<>();

    private PlannerUtil() {}

    public static Map<UUID, List<Long>> plan(
            List<ConsumerTxScope> transactions,
            CommittedTransactions committed,
            List<Long> inProgress) {
        long currentId = committed.getLastDenseCommit();

        Set<Map.Entry<String, List>> blocked = new HashSet<>();
        Map<UUID, Set<Map.Entry<String, List>>> claimed = new HashMap<>();
        Map<UUID, List<Long>> plan = new HashMap<>();
        for (ConsumerTxScope info : transactions) {
            long id = info.getTransactionId();
            if (id > currentId) {
                break;
            }
            if (!committed.contains(id)) {
                List<Map.Entry<String, List>> scope = info.getScope();
                if (inProgress.contains(id) || scope.stream().anyMatch(blocked::contains)) {
                    blocked.addAll(scope);
                } else {
                    UUID consumerId = info.getConsumerId();
                    if (isIntersectedWithClaimed(consumerId, scope, claimed)) {
                        blocked.addAll(scope);
                    } else {
                        claimed.computeIfAbsent(consumerId, CLAIMED).addAll(scope);
                        plan.computeIfAbsent(consumerId, READY).add(id);
                    }
                }
            }
            currentId++;
        }
        return plan;
    }

    private static boolean isIntersectedWithClaimed(
            UUID consumerId,
            List<Map.Entry<String, List>> scope,
            Map<UUID, Set<Map.Entry<String, List>>> claimed) {
        return claimed.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(consumerId))
                .anyMatch(entry -> scope.stream().anyMatch(e -> entry.getValue().contains(e)));
    }
}
