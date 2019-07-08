package com.salesforce.dynamodbv2.mt.mappers;

import static com.google.common.collect.Iterables.getLast;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import com.salesforce.dynamodbv2.mt.util.StreamArn.MtStreamArn;
import java.util.ArrayList;
import java.util.List;

/**
 * Table per tenant type streams implementation.
 */
class MtAmazonDynamoDbStreamsByTable extends MtAmazonDynamoDbStreamsBase<MtAmazonDynamoDbByTable> implements
    MtAmazonDynamoDbStreams {

    MtAmazonDynamoDbStreamsByTable(AmazonDynamoDBStreams streams, MtAmazonDynamoDbByTable mtDynamoDb) {
        super(streams, mtDynamoDb);
    }

    @Override
    protected MtGetRecordsResult getMtRecords(GetRecordsRequest request, StreamArn streamArn) {
        final String[] tenantAndTableName = mtDynamoDb.getTenantAndTableName(streamArn.getTableName());
        return getMtRecords(request, tenantAndTableName[0], tenantAndTableName[1]);
    }

    @Override
    protected MtGetRecordsResult getMtRecords(GetRecordsRequest request, MtStreamArn mtStreamArn) {
        return getMtRecords(request, mtStreamArn.getContext(), mtStreamArn.getTenantTableName());
    }

    private final MtGetRecordsResult getMtRecords(GetRecordsRequest request, String tenant, String tenantTable) {
        final GetRecordsResult result = dynamoDbStreams.getRecords(request);
        final List<Record> records = result.getRecords();
        final String nextIterator = result.getNextShardIterator();
        if (records.isEmpty()) {
            return new MtGetRecordsResult().withRecords(records).withNextShardIterator(nextIterator);
        }
        final List<Record> mtRecords = new ArrayList<>(records.size());
        for (Record record : records) {
            mtRecords.add(mapRecord(tenant, tenantTable, record));
        }
        return new MtGetRecordsResult()
            .withRecords(mtRecords)
            .withNextShardIterator(nextIterator)
            .withLastSequenceNumber(getLast(mtRecords).getDynamodb().getSequenceNumber());
    }

    private MtRecord mapRecord(String tenant, String tableName, Record record) {
        return new MtRecord()
            .withAwsRegion(record.getAwsRegion())
            .withEventID(record.getEventID())
            .withEventName(record.getEventName())
            .withEventSource(record.getEventSource())
            .withEventVersion(record.getEventVersion())
            .withContext(tenant)
            .withTableName(tableName)
            .withDynamodb(record.getDynamodb());
    }

}
