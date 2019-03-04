package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class SharedTableBuilderTest {

    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";

    AmazonDynamoDB localDynamoDB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

    private static final String tablePrefix = "oktodelete-testBillingMode.";
    String tableName;
    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
            new MtAmazonDynamoDbContextProviderThreadLocalImpl();

    @BeforeEach
    void beforeEach() {
        tableName = new String(String.valueOf(System.currentTimeMillis()));
    }

    private static List<String> testTables = new ArrayList<>(Arrays.asList("mt_sharedtablestatic_s_s",
            "mt_sharedtablestatic_s_n", "mt_sharedtablestatic_s_b", "mt_sharedtablestatic_s_nolsi",
            "mt_sharedtablestatic_s_s_nolsi", "mt_sharedtablestatic_s_n_nolsi",
            "mt_sharedtablestatic_s_b_nolsi")).stream()
            .map(testTable -> tablePrefix + testTable).collect(Collectors.toList());

    private void verifyBillingModeIsProvisioned() {
        for (String table: testTables) {
            assertEquals(1, localDynamoDB.describeTable(table)
                    .getTable().getProvisionedThroughput().getWriteCapacityUnits().intValue());
            assertEquals(1, localDynamoDB.describeTable(table)
                    .getTable().getProvisionedThroughput().getReadCapacityUnits().intValue());

            assert (BillingMode.PROVISIONED.toString().equals(localDynamoDB.describeTable(
                    table).getTable().getBillingModeSummary().getBillingMode()));
        }
    }

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

        assertEquals(1, localDynamoDB.describeTable(tablePrefix + tableName)
                    .getTable().getProvisionedThroughput().getWriteCapacityUnits().intValue());
        assertEquals(1, localDynamoDB.describeTable(tablePrefix + tableName)
                    .getTable().getProvisionedThroughput().getReadCapacityUnits().intValue());
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

        verifyBillingModeIsProvisioned();
    }

    @Test
    void testBillingModeProvisionedThroughputIsSetForDefaultCreateTableRequestsWithNullInputBillingMode() {
        SharedTableBuilder.builder()
                .withAmazonDynamoDb(localDynamoDB)
                .withTablePrefix(tablePrefix)
                .withPrecreateTables(true)
                .withContext(MT_CONTEXT)
                .build();

        verifyBillingModeIsProvisioned();
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

        assert (BillingMode.PAY_PER_REQUEST.toString().equals(localDynamoDB.describeTable(
                tablePrefix + tableName).getTable().getBillingModeSummary().getBillingMode()));
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
            assert (BillingMode.PAY_PER_REQUEST.toString().equals(localDynamoDB.describeTable(
                    table).getTable().getBillingModeSummary().getBillingMode()));
        }
    }
}