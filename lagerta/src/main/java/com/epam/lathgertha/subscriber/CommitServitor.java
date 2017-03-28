package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.kafka.KafkaLogCommitter;
import com.epam.lathgertha.util.Serializer;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Utilize logic of single transaction commit and write to local kafka log
 */
public class CommitServitor {
    private final Committer committer;
    private final Serializer serializer;
    private final KafkaLogCommitter kafkaLogCommitter;

    public CommitServitor(Serializer serializer, Committer committer, KafkaLogCommitter kafkaLogCommitter) {
        this.serializer = serializer;
        this.committer = committer;
        this.kafkaLogCommitter = kafkaLogCommitter;
    }

    @SuppressWarnings("unchecked")
    protected void commit(Long txId, Map<Long, Map.Entry<TransactionScope, ByteBuffer>> buffer) {
        Map.Entry<TransactionScope, ByteBuffer> transactionScopeAndSerializedValues = buffer.get(txId);
        List<Map.Entry<String, List>> scope = transactionScopeAndSerializedValues.getKey().getScope();

        Iterator<String> cacheNames = scope.stream().map(Map.Entry::getKey).iterator();
        Iterator<List> keys = scope.stream().map(Map.Entry::getValue).iterator();
        Iterator values = serializer.<List>deserialize(transactionScopeAndSerializedValues.getValue()).iterator();

        committer.commit(cacheNames, keys, values);
        kafkaLogCommitter.commitTransaction(txId);
    }
}
