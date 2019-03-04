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
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByTable;
import com.salesforce.dynamodbv2.mt.util.DynamoDbTestUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SharedTableBuilderTest {

    AmazonDynamoDB localDynamoDB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    CreateTableRequest request;
    MtAmazonDynamoDbByTable.MtAmazonDynamoDbBuilder mtDynamoDbByTableBuilder;
    String tableName;

    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
            new MtAmazonDynamoDbContextProviderThreadLocalImpl();
    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";
    private static final String tablePrefix = "oktodelete-testBillingMode.";

    @BeforeEach
    void beforeEach() {
        tableName = new String(String.valueOf(System.currentTimeMillis()));
    }

    private static List<String> testTables = new ArrayList<>(Arrays.asList("mt_sharedtablestatic_s_s",
            "mt_sharedtablestatic_s_n", "mt_sharedtablestatic_s_b", "mt_sharedtablestatic_s_nolsi",
            "mt_sharedtablestatic_s_s_nolsi", "mt_sharedtablestatic_s_n_nolsi",
            "mt_sharedtablestatic_s_b_nolsi")).stream()
            .map(testTable -> tablePrefix + testTable).collect(Collectors.toList());

    @AfterEach
    void tearDown() {
        for (String tableName: localDynamoDB.listTables().getTableNames()) {
            localDynamoDB.deleteTable(tableName);
        }
    }

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

        SharedTableBuilder.builder()
                .withBillingMode(BillingMode.PROVISIONED)
                .withCreateTableRequests(request)
                .withStreamsEnabled(false)
                .withPrecreateTables(true)
                .withTablePrefix(tablePrefix)
                .withAmazonDynamoDb(localDynamoDB)
                .withContext(MT_CONTEXT)
                .build();

        DynamoDbTestUtils.assertProvisionedIsSet(DynamoDbTestUtils.getTableNameWithPrefix(tablePrefix, tableName,
                ""), localDynamoDB, 1L);
    }

    @Test
    void testBillingModeProvisionedThroughputIsSetForDefaultCreateTableRequestsWithProvisionedInputBillingMode() {
        SharedTableBuilder.builder()
                .withBillingMode(BillingMode.PROVISIONED)
                .withAmazonDynamoDb(localDynamoDB)
                .withTablePrefix(tablePrefix)
                .withPrecreateTables(true)
                .withContext(MT_CONTEXT)
                .build();

        DynamoDbTestUtils.assertProvisionedIsSetForSetOfTables(testTables, localDynamoDB, 1L);
    }

    @Test
    void testBillingModeProvisionedThroughputIsSetForDefaultCreateTableRequestsWithNullInputBillingMode() {
        SharedTableBuilder.builder()
                .withAmazonDynamoDb(localDynamoDB)
                .withTablePrefix(tablePrefix)
                .withPrecreateTables(true)
                .withContext(MT_CONTEXT)
                .build();

        DynamoDbTestUtils.assertProvisionedIsSetForSetOfTables(testTables, localDynamoDB, 1L);
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

        SharedTableBuilder.builder()
                .withBillingMode(BillingMode.PAY_PER_REQUEST)
                .withCreateTableRequests(request)
                .withStreamsEnabled(false)
                .withPrecreateTables(true)
                .withTablePrefix(tablePrefix)
                .withAmazonDynamoDb(localDynamoDB)
                .withContext(MT_CONTEXT)
                .build();

        DynamoDbTestUtils.assertPayPerRequestIsSet(DynamoDbTestUtils.getTableNameWithPrefix(tablePrefix, tableName,
                ""), localDynamoDB);
    }

    @Test
    void testBillingModeIsPayPerRequestForDefaultCreateTableRequestsWithPayPerRequestInputBillingMode() {
        SharedTableBuilder.builder()
                .withBillingMode(BillingMode.PAY_PER_REQUEST)
                .withAmazonDynamoDb(localDynamoDB)
                .withTablePrefix(tablePrefix)
                .withPrecreateTables(true)
                .withContext(MT_CONTEXT)
                .build();

        for (String table: testTables) {
            DynamoDbTestUtils.assertPayPerRequestIsSet(table, localDynamoDB);
        }
    }
}