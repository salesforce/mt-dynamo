package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import com.salesforce.dynamodbv2.mt.util.StreamArn.MtStreamArn;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.function.Function;

/**
 * Table per tenant type streams implementation.
 */
class MtAmazonDynamoDbStreamsByTable extends MtAmazonDynamoDbStreamsBase<MtAmazonDynamoDbByTable> implements
    MtAmazonDynamoDbStreams {

    private final Timer getAllRecordsTime;
    private final DistributionSummary getAllRecordsSize;
    private final Timer getRecordsTime;
    private final DistributionSummary getRecordsSize;

    MtAmazonDynamoDbStreamsByTable(AmazonDynamoDBStreams streams, MtAmazonDynamoDbByTable mtDynamoDb) {
        super(streams, mtDynamoDb);
        final MeterRegistry meterRegistry = mtDynamoDb.getMeterRegistry();
        final String name = MtAmazonDynamoDbStreamsByTable.class.getSimpleName();
        getRecordsTime = meterRegistry.timer(name + ".GetRecords.Time");
        getRecordsSize = meterRegistry.summary(name + ".GetRecords.Size");
        getAllRecordsTime = meterRegistry.timer(name + ".GetAllRecords.Time");
        getAllRecordsSize = meterRegistry.summary(name + ".GetAllRecords.Size");
    }

    /**
     * When called to retrieve all records, derive tenant context and table name from physical table name.
     */
    @Override
    protected MtGetRecordsResult getAllRecords(GetRecordsRequest request, StreamArn streamArn) {
        return getAllRecordsTime.record(() -> {
            final String[] tenantAndTableName = mtDynamoDb.getTenantAndTableName(streamArn.getTableName());
            return getMtRecords(request, mapper(tenantAndTableName[0], tenantAndTableName[1]), getAllRecordsSize);
        });
    }

    /**
     * When called to retrieve records for a specific tenant, derive tenant context and table name from arn.
     */
    @Override
    protected MtGetRecordsResult getRecords(GetRecordsRequest request, MtStreamArn mtStreamArn) {
        return getRecordsTime.record(() ->
            getMtRecords(request, mapper(mtStreamArn.getContext(), mtStreamArn.getTenantTableName()), getRecordsSize));
    }

    private Function<Record, MtRecord> mapper(String tenant, String tableName) {
        return record -> mapRecord(tenant, tableName, record);
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
