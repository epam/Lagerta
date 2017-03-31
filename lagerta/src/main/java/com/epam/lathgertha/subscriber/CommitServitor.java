package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.capturer.TransactionScope;
import com.epam.lathgertha.kafka.KafkaLogCommitter;
import com.epam.lathgertha.services.LeadService;
import com.epam.lathgertha.util.Serializer;
import org.apache.ignite.Ignite;

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
    private final LeadService lead;

    public CommitServitor(Serializer serializer, Committer committer, KafkaLogCommitter kafkaLogCommitter, Ignite ignite) {
        this.serializer = serializer;
        this.committer = committer;
        this.kafkaLogCommitter = kafkaLogCommitter;
        this.lead = ignite.services().serviceProxy(LeadService.NAME, LeadService.class, false);
    }

    @SuppressWarnings("unchecked")
    public boolean commit(Long txId, Map<Long, Map.Entry<TransactionScope, ByteBuffer>> buffer) {
        try {
            Map.Entry<TransactionScope, ByteBuffer> transactionScopeAndSerializedValues = buffer.get(txId);
            List<Map.Entry<String, List>> scope = transactionScopeAndSerializedValues.getKey().getScope();

            Iterator<String> cacheNames = scope.stream().map(Map.Entry::getKey).iterator();
            Iterator<List> keys = scope.stream().map(Map.Entry::getValue).iterator();
            Iterator values = serializer.<List>deserialize(transactionScopeAndSerializedValues.getValue()).iterator();

            committer.commit(cacheNames, keys, values);
            kafkaLogCommitter.commitTransaction(txId);
            return true;
        } catch (Exception e) {
            lead.notifyFailed(txId);
            return false;
        }
    }
}
