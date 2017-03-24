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

package org.apache.ignite.activestore.impl.quasi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.inject.Inject;
import org.apache.ignite.activestore.KeyValueProvider;
import org.apache.ignite.activestore.Metadata;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Andrei_Yakushin
 * @since 8/23/2016 3:09 PM
 */
public class QuasiKafkaKeyValueProvider implements KeyValueProvider {
    public static final String CONTROL = "C";
    public static final String DATA = "D.";

    @Inject
    private Producer producer;

    @Inject
    private Serializer serializer;

    private final Function<Object, Integer> partitioner = new Function<Object, Integer>() {
        @Override
        public Integer apply(Object o) {
            return (o == null ? 0 : Math.abs(o.hashCode())) % 32;
        }
    };

    @Override
    public <K, V> V load(K key, String cacheName, Iterable<Metadata> path) throws CacheLoaderException {
        return null;
    }

    @Override
    public <K, V> Map<K, V> loadAll(Iterable<? extends K> keys, String cacheName, Iterable<Metadata> path) throws CacheLoaderException {
        return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(long transactionId, Map<String, Collection<Cache.Entry<?, ?>>> updates, Metadata metadata) throws CacheWriterException {
        boolean ok = false;
        Map<String, Long> dataOffsets = null;
        Integer controlPartition = null;
        Long transactionOffset = null;
        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord(CONTROL, serializer.serialize(new TransactionMessage((byte) 0, metadata, null))));
            RecordMetadata recordMetadata = future.get();
            transactionOffset = recordMetadata.offset();
            Map<String, Future<RecordMetadata>> futures = new HashMap<>();

            controlPartition = recordMetadata.partition();
            for (Map.Entry<String, Collection<Cache.Entry<?, ?>>> i : updates.entrySet()) {
                Collection<Cache.Entry<?, ?>> value = i.getValue();
                Map<Integer, Collection<Cache.Entry<?, ?>>> byPartition = new HashMap<>();
                for (Cache.Entry<?, ?> j : value) {
                    Integer partition = partitioner.apply(j.getKey());
                    Collection<Cache.Entry<?, ?>> entries = byPartition.get(partition);
                    if (entries == null) {
                        entries = new ArrayList<>();
                        byPartition.put(partition, entries);
                    }
                    entries.add(j);
                }
                for (Map.Entry<Integer, Collection<Cache.Entry<?, ?>>> entry : byPartition.entrySet()) {
                    futures.put(i.getKey(), producer.send(new ProducerRecord(
                            DATA + i.getKey(),
                            entry.getKey(),
                            transactionOffset,
                            serializer.serialize(entry.getValue())
                    )));
                }
            }
            dataOffsets = new HashMap<>(futures.size());
            for (Map.Entry<String, Future<RecordMetadata>> entry : futures.entrySet()) {
                dataOffsets.put(entry.getKey(), entry.getValue().get().offset());
            }

            ok = true;
        } catch (InterruptedException | ExecutionException e) {
            throw new CacheWriterException(e);
        } finally {
            if (transactionOffset != null) {
                byte code = (byte) (ok ? 1 : 2);
                producer.send(new ProducerRecord(
                        CONTROL,
                        controlPartition,
                        null,
                        transactionOffset,
                        serializer.serialize(new TransactionMessage(code, metadata, dataOffsets))
                ));
            }
        }
    }

    @Override
    public void fetchAllKeys(String cacheName, Metadata metadata, IgniteInClosure<Object> action) throws CacheLoaderException {
        //do nothing;
    }

    @Override
    public void fetchAllKeyValues(String cacheName, Metadata metadata, IgniteInClosure<Cache.Entry<Object, Object>> action) {
        //do nothing;
    }

    @Override
    public Map<String, List<Metadata>> getSnapshotsByCache(Iterable<Metadata> metadatas) {
        return Collections.emptyMap();
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class TransactionMessage implements Serializable {
        private final byte type;
        private final Metadata metadata;
        private final Map<String, Long> dataOffsets;

        public TransactionMessage(byte type, Metadata metadata, Map<String, Long> dataOffsets) {
            this.type = type;
            this.metadata = metadata;
            this.dataOffsets = dataOffsets;
        }

        public byte getType() {
            return type;
        }

        public Metadata getMetadata() {
            return metadata;
        }

        public Map<String, Long> getDataOffsets() {
            return dataOffsets;
        }
    }

}
