package com.epam.lathgertha.subscriber;

import com.epam.lathgertha.kafka.KafkaLogCommitter;
import com.epam.lathgertha.services.LeadService;
import com.epam.lathgertha.util.Serializer;
import org.apache.ignite.Ignite;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Utilize logic of single transaction commit and write to local kafka log
 */
public class CommitServitor {
    private final Committer committer;
    private final Serializer serializer;
    private final KafkaLogCommitter kafkaLogCommitter;
    private final LeadService lead;
    private final UUID readerId;

    public CommitServitor(Serializer serializer, Committer committer, KafkaLogCommitter kafkaLogCommitter,
                          UUID readerId, Ignite ignite) {
        this.serializer = serializer;
        this.committer = committer;
        this.kafkaLogCommitter = kafkaLogCommitter;
        this.readerId = readerId;
        lead = ignite.services().serviceProxy(LeadService.NAME, LeadService.class, false);
    }

    @SuppressWarnings("unchecked")
    public boolean commit(Long txId, Map<Long, TransactionData> buffer) {
        try {
            TransactionData transactionScopeAndSerializedValues = buffer.get(txId);
            List<Map.Entry<String, List>> scope = transactionScopeAndSerializedValues.getTransactionScope().getScope();

            Iterator<String> cacheNames = scope.stream().map(Map.Entry::getKey).iterator();
            Iterator<List> keys = scope.stream().map(Map.Entry::getValue).iterator();
            Iterator values = serializer.<List>deserialize(transactionScopeAndSerializedValues.getValue()).iterator();

            committer.commit(cacheNames, keys, values);
            kafkaLogCommitter.commitTransaction(txId);
            return true;
        } catch (Exception e) {
            lead.notifyFailed(readerId, txId);
            return false;
        }
    }
}
