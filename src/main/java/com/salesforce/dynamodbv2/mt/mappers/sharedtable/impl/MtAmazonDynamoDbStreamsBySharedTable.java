package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreamsBase;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldPrefixFunction.FieldValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MtAmazonDynamoDbStreamsBySharedTable extends MtAmazonDynamoDbStreamsBase<MtAmazonDynamoDbBySharedTable> {

    private static final Logger LOG = LoggerFactory.getLogger(MtAmazonDynamoDbStreamsBySharedTable.class);
    private static final int MAX_LIMIT = 1000;

    /**
     * Default constructor.
     *
     * @param dynamoDbStreams underlying streams instance
     * @param mtDynamoDb corresponding shared table dynamo DB instance
     */
    public MtAmazonDynamoDbStreamsBySharedTable(AmazonDynamoDBStreams dynamoDbStreams,
        MtAmazonDynamoDbBySharedTable mtDynamoDb) {
        super(dynamoDbStreams, mtDynamoDb);
    }

    @Override
    protected GetRecordsResult getRecords(Function<Record, MtRecord> recordMapper, GetRecordsRequest request) {
        GetRecordsResult result = dynamoDbStreams.getRecords(request);
        GetRecordsResult mtResult = mapResult(recordMapper, result);

        // keep fetching records if we haven't reached the limit yet
        int limit = Optional.ofNullable(request.getLimit()).orElse(MAX_LIMIT);
        while (mtResult.getRecords().size() < limit
            && !result.getRecords().isEmpty()
            && result.getNextShardIterator() != null) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("Fetching more records. limit={}, current={}", limit, mtResult.getRecords().size());
            }

            GetRecordsRequest nextRequest = new GetRecordsRequest()
                .withShardIterator(result.getNextShardIterator())
                .withLimit(limit);
            GetRecordsResult nextResult = dynamoDbStreams.getRecords(nextRequest);
            GetRecordsResult nextMtResult = mapResult(recordMapper, nextResult);
            int unionSize = mtResult.getRecords().size() + nextMtResult.getRecords().size();
            if (unionSize > limit) {
                break;
            }
            result = nextResult;
            List<Record> union = new ArrayList<>(unionSize);
            union.addAll(mtResult.getRecords());
            union.addAll(nextMtResult.getRecords());
            mtResult = nextMtResult.withRecords(union);
        }

        return mtResult;
    }

    @Override
    protected Function<Record, MtRecord> getMtRecordMapper(String tableName) {
        Function<Map<String, AttributeValue>, FieldValue> fieldValueFunction =
            mtDynamoDb.getFieldValueFunction(tableName);
        return record -> mapRecord(fieldValueFunction, record);
    }

    private MtRecord mapRecord(Function<Map<String, AttributeValue>, FieldValue> fieldValueFunction,
        Record record) {
        FieldValue fieldValue = fieldValueFunction.apply(record.getDynamodb().getKeys());
        MtAmazonDynamoDbContextProvider mtContext = mtDynamoDb.getMtContext();
        // execute in record tenant context to get table mapping
        TableMapping tableMapping = mtContext.withContext(fieldValue.getMtContext(),
            mtDynamoDb::getTableMapping, fieldValue.getTableIndex());
        ItemMapper itemMapper = tableMapping.getItemMapper();
        StreamRecord streamRecord = record.getDynamodb();
        return new MtRecord()
            .withAwsRegion(record.getAwsRegion())
            .withEventID(record.getEventID())
            .withEventName(record.getEventName())
            .withEventSource(record.getEventSource())
            .withEventVersion(record.getEventVersion())
            .withContext(fieldValue.getMtContext())
            .withTableName(fieldValue.getTableIndex())
            .withDynamodb(new StreamRecord()
                .withKeys(itemMapper.reverse(streamRecord.getKeys()))
                .withNewImage(itemMapper.reverse(streamRecord.getNewImage()))
                .withOldImage(itemMapper.reverse(streamRecord.getOldImage()))
                .withSequenceNumber(streamRecord.getSequenceNumber())
                .withStreamViewType(streamRecord.getStreamViewType())
                .withApproximateCreationDateTime(streamRecord.getApproximateCreationDateTime())
                .withSizeBytes(streamRecord.getSizeBytes()));
    }

}
