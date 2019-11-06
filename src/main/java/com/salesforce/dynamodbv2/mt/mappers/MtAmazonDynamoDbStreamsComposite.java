/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static com.google.common.base.Preconditions.checkState;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.salesforce.dynamodbv2.mt.util.StreamArn;
import com.salesforce.dynamodbv2.mt.util.StreamArn.MtStreamArn;
import java.util.Map;

/**
 * An {@link MtAmazonDynamoDbStreams} that determines which of multiple {@link MtAmazonDynamoDbStreams}s to delegate to
 * based on context.
 */
public class MtAmazonDynamoDbStreamsComposite extends MtAmazonDynamoDbStreamsBase<MtAmazonDynamoDbComposite> {

    private final Map<AmazonDynamoDB, MtAmazonDynamoDbStreamsBase> streamsPerDelegateDb;

    public MtAmazonDynamoDbStreamsComposite(AmazonDynamoDBStreams streams, MtAmazonDynamoDbComposite mtDynamoDb,
                                            Map<AmazonDynamoDB, MtAmazonDynamoDbStreamsBase> streamsPerDelegateDb) {
        super(streams, mtDynamoDb);
        this.streamsPerDelegateDb = streamsPerDelegateDb;
    }

    @Override
    protected MtGetRecordsResult getRecords(GetRecordsRequest request, MtStreamArn mtStreamArn) {
        AmazonDynamoDB dbForContext = mtDynamoDb.getDelegateFromContext();
        MtAmazonDynamoDbStreamsBase streamsForContext = streamsPerDelegateDb.get(dbForContext);
        checkState(streamsForContext != null, "Base db client not mapped to a streams client");
        return streamsForContext.getRecords(request, mtStreamArn);
    }

    @Override
    protected MtGetRecordsResult getAllRecords(GetRecordsRequest request, StreamArn streamArn) {
        AmazonDynamoDB dbForPhysicalTable = mtDynamoDb.getDelegateFromPhysicalTableName(streamArn.getTableName());
        MtAmazonDynamoDbStreamsBase streamsForPhysicalTable = streamsPerDelegateDb.get(dbForPhysicalTable);
        checkState(streamsForPhysicalTable != null, "Base db client not mapped to a streams client");
        return streamsForPhysicalTable.getAllRecords(request, streamArn);
    }
}
