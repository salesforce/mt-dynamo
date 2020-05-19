package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreamsBase;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import com.salesforce.dynamodbv2.mt.util.StreamArn.MtStreamArn;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class MtAmazonDynamoDbStreamsBySharedTable extends MtAmazonDynamoDbStreamsBase<MtAmazonDynamoDbBySharedTable> {

    private final SharedTableStreamsMappingDelegate delegate;

    // TODO move to common class
    static final int MAX_LIMIT = 1000;

    private final Timer getAllRecordsTime;
    private final DistributionSummary getAllRecordsSize;

    /**
     * Default constructor.
     *
     * @param dynamoDbStreams underlying streams instance
     * @param mtDynamoDb      corresponding shared table DynamoDB instance
     */
    public MtAmazonDynamoDbStreamsBySharedTable(AmazonDynamoDBStreams dynamoDbStreams,
                                                MtAmazonDynamoDbBySharedTable mtDynamoDb) {
        super(dynamoDbStreams, mtDynamoDb);

        this.delegate = new SharedTableStreamsMappingDelegate(
            dynamoDbStreams,
            mtDynamoDb.getClock(),
            mtDynamoDb.getGetRecordsTimeLimit(),
            mtDynamoDb.getMeterRegistry());

        final MeterRegistry meterRegistry = mtDynamoDb.getMeterRegistry();
        final String name = MtAmazonDynamoDbStreamsBySharedTable.class.getSimpleName();
        getAllRecordsTime = meterRegistry.timer(name + ".GetAllRecords.Time");
        getAllRecordsSize = meterRegistry.summary(name + ".GetAllRecords.Size");
    }

    private TableMapping getTableMapping(MtStreamArn arn) {
        return mtDynamoDb.getTableMapping(arn.getTenantTableName()).get();
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        return super.getShardIterator(getShardIteratorRequest);
    }

    /**
     * To get all records, use key prefix to determine how to map records. Does not check whether streaming is enabled
     * for a given tenant table, since the consumers of this API (such as KCL) currently expect to see all records in
     * the physical stream. It could be argued that this somewhat breaks the abstraction, since the client now has to
     * check on its end whether the virtual table for a given record has streaming enabled. On the other hand, we are
     * already breaking the abstraction by allowing calling without context, so hopefully that's acceptable.
     */
    @Override
    protected MtGetRecordsResult getAllRecords(GetRecordsRequest request, StreamArn streamArn) {
        return getAllRecordsTime.record(() -> {
            final MtGetRecordsResult result = getMtRecords(request, getRecordMapper(streamArn.getTableName()));
            getAllRecordsSize.record(result.getStreamSegmentMetrics().getRecordCount());
            return result;
        });
    }

    private Function<Record, Optional<MtRecord>> getRecordMapper(String physicalTableName) {
        // get function that extracts tenant context and table name from key prefix
        final Function<Map<String, AttributeValue>, MtContextAndTable> contextParser =
            mtDynamoDb.getContextParser(physicalTableName);
        return record -> {
            // when called to map a record, extract tenant context and table
            final MtContextAndTable mtContextAndTable = contextParser.apply(record.getDynamodb().getKeys());
            // then establish context to map physical to virtual record, i.e., remove index key prefixes
            return mtDynamoDb.getMtContext().withContext(mtContextAndTable.getContext(), () -> {
                Optional<TableMapping> tableMapping = mtDynamoDb.getTableMapping(mtContextAndTable.getTableName());
                return tableMapping.map(TableMapping::getRecordMapper)
                    .map(f -> f.apply(record));
            });
        };
    }

    @Override
    protected MtGetRecordsResult getRecords(GetRecordsRequest request, MtStreamArn mtStreamArn) {
        return delegate.getMtRecords(getTableMapping(mtStreamArn), request, mtStreamArn);
    }

}
