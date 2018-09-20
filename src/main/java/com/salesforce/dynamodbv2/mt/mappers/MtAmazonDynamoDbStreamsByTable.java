package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table per tenant type streams implementation.
 */
class MtAmazonDynamoDbStreamsByTable extends MtAmazonDynamoDbStreamsBase<MtAmazonDynamoDbByTable> implements
    MtAmazonDynamoDbStreams {

    private static final Logger LOG = LoggerFactory.getLogger(MtAmazonDynamoDbStreamsByTable.class);

    MtAmazonDynamoDbStreamsByTable(AmazonDynamoDBStreams streams, MtAmazonDynamoDbByTable mtDynamoDb) {
        super(streams, mtDynamoDb);
    }

    @Override
    protected Function<Record, MtRecord> getMtRecordMapper(String tableName) {
        String[] tenantAndTableName = mtDynamoDb.getTenantAndTableName(tableName);
        return record -> mapRecord(tenantAndTableName[0], tenantAndTableName[1], record);
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
