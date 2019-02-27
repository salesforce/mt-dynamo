package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SharedTableBuilderIt {

    /**
     * These tests run against a remote instance of DynamoDB, due to limitations in the LocalDynamoDBClient
     * which does not allow BillingMode PAY_PER_REQUEST to be enabled without specifying throughput
     * (which differs from what the DynamoDB API allows).
     */

    static final Regions REGION = Regions.US_EAST_1;
    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";

    private static AmazonDynamoDB remoteDynamoDB = AmazonDynamoDBClientBuilder.standard()
        .withCredentials(new EnvironmentVariableCredentialsProvider())
            .withRegion(REGION).build();

    private static final String TABLE_PREFIX = "oktodelete-testBillingMode.";
    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
            new MtAmazonDynamoDbContextProviderThreadLocalImpl();

    private static final Logger LOG = LoggerFactory.getLogger(SharedTableBuilderIt.class);

    private static List<String> testTables = new ArrayList<>(Arrays.asList("mt_sharedtablestatic_s_s",
            "mt_sharedtablestatic_s_n", "mt_sharedtablestatic_s_b", "mt_sharedtablestatic_s_nolsi",
            "mt_sharedtablestatic_s_s_nolsi", "mt_sharedtablestatic_s_n_nolsi",
            "mt_sharedtablestatic_s_b_nolsi")).stream()
            .map(testTable -> TABLE_PREFIX + testTable).collect(Collectors.toList());

    @Test
    void testBillingModePayPerRequestIsSetForCustomCreateTableRequests() {
        String tableName = new String(String.valueOf(System.currentTimeMillis()));
        String fullTableName = TABLE_PREFIX + tableName;
        testTables.add(fullTableName);

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
                .withTablePrefix(TABLE_PREFIX)
                .withAmazonDynamoDb(remoteDynamoDB)
                .withContext(MT_CONTEXT)
                .build();

        assertEquals(BillingMode.PAY_PER_REQUEST.toString(), remoteDynamoDB.describeTable(fullTableName)
                .getTable().getBillingModeSummary().getBillingMode());
    }

    @Test
    void testBillingModePayPerRequestIsSetForDefaultCreateTableRequests() {

        SharedTableBuilder.builder()
                .withBillingMode(BillingMode.PAY_PER_REQUEST)
                .withStreamsEnabled(false)
                .withPrecreateTables(true)
                .withTablePrefix(TABLE_PREFIX)
                .withAmazonDynamoDb(remoteDynamoDB)
                .withContext(MT_CONTEXT)
                .build();

        for (String table: testTables) {
            assertEquals(BillingMode.PAY_PER_REQUEST.toString(), remoteDynamoDB.describeTable(
                    table).getTable().getBillingModeSummary().getBillingMode());
        }
    }

    @AfterAll
    static void afterAll() throws InterruptedException {
        Thread.sleep(5000);
        for (String table: testTables) {
            try {
                remoteDynamoDB.deleteTable(table);
            } catch (ResourceInUseException e) {
                LOG.info("table in use: " + table);
            } catch (ResourceNotFoundException e) {
                LOG.error("table not found: " + table);
            }
        }
    }
}