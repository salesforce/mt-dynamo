package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreams;

import java.util.Set;


public class MtAmazonDynamoDbStreamsBySharedTable extends DelegatingAmazonDynamoDbStreams implements
        MtAmazonDynamoDbStreams {

    private MtAmazonDynamoDbBySharedTable mtDynamoDb;

    /**
     * Default constructor.
     *
     * @param amazonDynamoDbStreams underlying streams instance
     * @param mtDynamoDb            corresponding shared table dynamo DB instance
     */
    public MtAmazonDynamoDbStreamsBySharedTable(AmazonDynamoDBStreams amazonDynamoDbStreams,
                                                MtAmazonDynamoDbBySharedTable mtDynamoDb) {
        super(amazonDynamoDbStreams instanceof CachingAmazonDynamoDbStreams ? amazonDynamoDbStreams :
                new CachingAmazonDynamoDbStreams.Builder(amazonDynamoDbStreams).build());
        this.mtDynamoDb = mtDynamoDb;
    }

    /**
     * Returns streams associated with the corresponding MT shared table instance.
     *
     * @param listStreamsRequest Stream request. Currently doesn't support filtering by table.
     * @return Result.
     */
    @Override
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
        checkArgument(mtDynamoDb.getMtContext().getContext() == null,
                "listStreams for shared table currently does not support calling with tenant context");
        checkArgument(listStreamsRequest.getTableName() == null,
                "listStreams for shared table currently does not support filtering by table name");

        ListStreamsResult result = super.listStreams(listStreamsRequest);
        Set<String> sharedTables =
                mtDynamoDb.getSharedTables().stream().map(CreateTableRequest::getTableName).collect(toSet());
        return result.withStreams(result.getStreams().stream()
                .filter(stream -> sharedTables.contains(stream.getTableName()))
                .collect(toList()));
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AmazonDynamoDBStreams getAmazonDynamoDbStreams() {
        return delegate;
    }

}
