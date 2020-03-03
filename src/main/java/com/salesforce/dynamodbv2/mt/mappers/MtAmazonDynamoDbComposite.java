/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An {@link MtAmazonDynamoDb} that determines which of multiple {@link MtAmazonDynamoDb}s to delegate to based on
 * context.
 *
 * <p>TODO: support backup-related functionality
 */
public class MtAmazonDynamoDbComposite extends MtAmazonDynamoDbBase {

    private final Collection<MtAmazonDynamoDbBase> delegates;
    private final Supplier<MtAmazonDynamoDbBase> fromContextToDelegate;
    private final Function<String, MtAmazonDynamoDbBase> fromPhysicalTableNameToDelegate;

    public MtAmazonDynamoDbComposite(Collection<MtAmazonDynamoDbBase> delegates,
                                     Supplier<MtAmazonDynamoDbBase> fromContextToDelegate,
                                     Function<String, MtAmazonDynamoDbBase> fromPhysicalTableNameToDelegate) {
        super(validateDelegatesAndGetFirst(delegates).getMtContext(),
            delegates.iterator().next().getAmazonDynamoDb(),
            delegates.iterator().next().getMeterRegistry(),
            delegates.iterator().next().getScanTenantKey(),
            delegates.iterator().next().getScanVirtualTableKey());
        this.delegates = delegates;
        this.fromContextToDelegate = fromContextToDelegate;
        this.fromPhysicalTableNameToDelegate = fromPhysicalTableNameToDelegate;
    }

    private static MtAmazonDynamoDbBase validateDelegatesAndGetFirst(Collection<MtAmazonDynamoDbBase> delegates) {
        checkArgument(delegates != null && !delegates.isEmpty(), "Must provide at least one delegate");
        Iterator<MtAmazonDynamoDbBase> iterator = delegates.iterator();
        MtAmazonDynamoDbBase first = iterator.next();
        while (iterator.hasNext()) {
            MtAmazonDynamoDbBase next = iterator.next();
            checkArgument(Objects.equals(first.getMtContext(), next.getMtContext()),
                "Delegates must share the same mt context provider");
            checkArgument(Objects.equals(disregardLogger(first.getAmazonDynamoDb()),
                disregardLogger(next.getAmazonDynamoDb())),
                "Delegates must share the same parent AmazonDynamoDB");
            checkArgument(Objects.equals(first.getMeterRegistry(), next.getMeterRegistry()),
                "Delegates must share the same meter registry");
            checkArgument(Objects.equals(first.getScanTenantKey(), next.getScanTenantKey()),
                "Delegates must share the same scan tenant key");
            checkArgument(Objects.equals(first.getScanVirtualTableKey(), next.getScanVirtualTableKey()),
                "Delegates must share the same scan virtual table key");
        }
        return first;
    }

    private static AmazonDynamoDB disregardLogger(AmazonDynamoDB amazonDynamoDB) {
        return amazonDynamoDB instanceof MtAmazonDynamoDbLogger
            ? ((MtAmazonDynamoDbLogger) amazonDynamoDB).getAmazonDynamoDb()
            : amazonDynamoDB;
    }

    Collection<MtAmazonDynamoDbBase> getDelegates() {
        return delegates;
    }

    MtAmazonDynamoDbBase getDelegateFromContext() {
        return fromContextToDelegate.get();
    }

    MtAmazonDynamoDbBase getDelegateFromPhysicalTableName(String physicalTableName) {
        return fromPhysicalTableNameToDelegate.apply(physicalTableName);
    }

    @Override
    protected boolean isMtTable(String tableName) {
        for (MtAmazonDynamoDbBase delegate : delegates) {
            if (delegate.isMtTable(tableName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) {
        return getDelegateFromContext().batchGetItem(batchGetItemRequest);
    }

    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        return getDelegateFromContext().createTable(createTableRequest);
    }

    @Override
    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
        return getDelegateFromContext().deleteItem(deleteItemRequest);
    }

    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        return getDelegateFromContext().deleteTable(deleteTableRequest);
    }

    @Override
    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
        return getDelegateFromContext().describeTable(describeTableRequest);
    }

    @Override
    public GetItemResult getItem(GetItemRequest getItemRequest) {
        return getDelegateFromContext().getItem(getItemRequest);
    }

    @Override
    public ListTablesResult listTables(String exclusiveStartTableName, Integer limit) {
        // delegate to MtAmazonDynamoDbBase's multitenant listTables call if no tenant is provided
        if (getMtContext().getContextOpt().isEmpty()) {
            return super.listTables(exclusiveStartTableName, limit);
        }
        return getDelegateFromContext().listTables(exclusiveStartTableName, limit);
    }

    @Override
    public PutItemResult putItem(PutItemRequest putItemRequest) {
        return getDelegateFromContext().putItem(putItemRequest);
    }

    @Override
    public QueryResult query(QueryRequest queryRequest) {
        return getDelegateFromContext().query(queryRequest);
    }

    @Override
    public ScanResult scan(ScanRequest scanRequest) {
        if (getMtContext().getContextOpt().isEmpty()) {
            // need to support cross-tenant scans -- use physical table name to find the right delegate
            return getDelegateFromPhysicalTableName(scanRequest.getTableName()).scan(scanRequest);
        }
        return getDelegateFromContext().scan(scanRequest);
    }

    @Override
    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
        return getDelegateFromContext().updateItem(updateItemRequest);
    }

}