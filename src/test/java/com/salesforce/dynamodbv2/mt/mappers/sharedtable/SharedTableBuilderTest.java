package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.util.DynamoDbTestUtils;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SharedTableBuilderTest {

    private static final AmazonDynamoDB LOCAL_DYNAMO_DB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
            new MtAmazonDynamoDbContextProviderThreadLocalImpl();
    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";
    private static final String TABLE_PREFIX_PREFIX = "oktodelete-testBillingMode.";
    private static String tablePrefix;
    private static final AtomicInteger counter = new AtomicInteger();
    private static String tableName;
    private static final String defaultTableName = "_tablemetadata";

    @BeforeEach
    void beforeEach() {
        tableName = String.valueOf(System.currentTimeMillis());
        tablePrefix = TABLE_PREFIX_PREFIX + counter.incrementAndGet();
    }

    private static final List<String> testTables = ImmutableList.of("mt_sharedtablestatic_s_s",
        "mt_sharedtablestatic_s_n", "mt_sharedtablestatic_s_b", "mt_sharedtablestatic_s_nolsi",
        "mt_sharedtablestatic_s_s_nolsi", "mt_sharedtablestatic_s_n_nolsi",
        "mt_sharedtablestatic_s_b_nolsi");

    @Test
    void testBillingModeProvisionedThroughputIsSetForCustomCreateTableRequests() {

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement(ID_ATTR_NAME, HASH))
                .withAttributeDefinitions(
                        new AttributeDefinition(ID_ATTR_NAME, S),
                        new AttributeDefinition(INDEX_ID_ATTR_NAME, S))
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                        .withIndexName("index")
                        .withKeySchema(new KeySchemaElement(INDEX_ID_ATTR_NAME, HASH))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L,1L))
                ).withProvisionedThroughput(new ProvisionedThroughput(1L,1L));

        SharedTableBuilder.sharedTableBuilder()
                .withBillingMode(BillingMode.PROVISIONED)
                .withCreateTableRequests(request)
                .withStreamsEnabled(false)
                .withPrecreateTables(true)
                .withTablePrefix(tablePrefix)
                .withAmazonDynamoDb(LOCAL_DYNAMO_DB)
                .withContext(MT_CONTEXT)
                .build();

        DynamoDbTestUtils.assertProvisionedIsSet(DynamoDbTestUtils.getTableNameWithPrefix(tablePrefix, tableName,
                ""), LOCAL_DYNAMO_DB, 1L);
        DynamoDbTestUtils.assertProvisionedIsSet(tablePrefix + defaultTableName, LOCAL_DYNAMO_DB, 1L);
    }

    @Test
    void testBillingModeProvisionedThroughputIsSetForDefaultCreateTableRequestsWithProvisionedInputBillingMode() {
        SharedTableBuilder.sharedTableBuilder()
                .withBillingMode(BillingMode.PROVISIONED)
                .withAmazonDynamoDb(LOCAL_DYNAMO_DB)
                .withTablePrefix(tablePrefix)
                .withPrecreateTables(true)
                .withContext(MT_CONTEXT)
                .build();

        DynamoDbTestUtils.assertProvisionedIsSetForSetOfTables(getPrefixedTables(), LOCAL_DYNAMO_DB,1L);
        DynamoDbTestUtils.assertProvisionedIsSet(tablePrefix + defaultTableName, LOCAL_DYNAMO_DB, 1L);
    }

    @Test
    void testBillingModeProvisionedThroughputIsSetForDefaultCreateTableRequestsWithNullInputBillingMode() {
        SharedTableBuilder.sharedTableBuilder()
                .withAmazonDynamoDb(LOCAL_DYNAMO_DB)
                .withTablePrefix(tablePrefix)
                .withPrecreateTables(true)
                .withContext(MT_CONTEXT)
                .build();

        DynamoDbTestUtils.assertProvisionedIsSetForSetOfTables(getPrefixedTables(), LOCAL_DYNAMO_DB, 1L);
        DynamoDbTestUtils.assertProvisionedIsSet(tablePrefix + defaultTableName, LOCAL_DYNAMO_DB, 1L);
    }

    @Test
    void testBillingModePayPerRequestThroughputIsSetForCustomCreateTableRequest() {
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement(ID_ATTR_NAME, HASH))
                .withAttributeDefinitions(
                        new AttributeDefinition(ID_ATTR_NAME, S),
                        new AttributeDefinition(INDEX_ID_ATTR_NAME, S))
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                        .withIndexName("index")
                        .withKeySchema(new KeySchemaElement(INDEX_ID_ATTR_NAME, HASH))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                );

        SharedTableBuilder.sharedTableBuilder()
                .withBillingMode(BillingMode.PAY_PER_REQUEST)
                .withCreateTableRequests(request)
                .withStreamsEnabled(false)
                .withPrecreateTables(true)
                .withTablePrefix(tablePrefix)
                .withAmazonDynamoDb(LOCAL_DYNAMO_DB)
                .withContext(MT_CONTEXT)
                .build();

        DynamoDbTestUtils.assertPayPerRequestIsSet(DynamoDbTestUtils.getTableNameWithPrefix(tablePrefix, tableName,
                ""), LOCAL_DYNAMO_DB);
        DynamoDbTestUtils.assertPayPerRequestIsSet(tablePrefix + defaultTableName, LOCAL_DYNAMO_DB);
    }

    @Test
    void testBillingModeIsPayPerRequestForDefaultCreateTableRequestsWithPayPerRequestInputBillingMode() {
        SharedTableBuilder.sharedTableBuilder()
                .withBillingMode(BillingMode.PAY_PER_REQUEST)
                .withAmazonDynamoDb(LOCAL_DYNAMO_DB)
                .withTablePrefix(tablePrefix)
                .withPrecreateTables(true)
                .withContext(MT_CONTEXT)
                .build();

        for (String table: getPrefixedTables()) {
            DynamoDbTestUtils.assertPayPerRequestIsSet(table, LOCAL_DYNAMO_DB);
        }

        DynamoDbTestUtils.assertPayPerRequestIsSet(tablePrefix + defaultTableName, LOCAL_DYNAMO_DB);
    }

    private List<String> getPrefixedTables() {
        return testTables.stream().map(testTable -> tablePrefix + testTable)
                .collect(Collectors.toList());
    }

}