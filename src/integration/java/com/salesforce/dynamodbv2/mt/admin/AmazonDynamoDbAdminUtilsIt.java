package com.salesforce.dynamodbv2.mt.admin;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import java.util.ArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AmazonDynamoDbAdminUtilsIt {

    /* This tests against hosted DynamoDB to ensure tables encounter TableInUse exception
    (local create times are too short) */

    private static final Regions REGION = Regions.US_EAST_1;
    private static final AmazonDynamoDB remoteDynamoDb = AmazonDynamoDBClientBuilder.standard()
        .withCredentials(new EnvironmentVariableCredentialsProvider())
        .withRegion(REGION).build();
    private static final AmazonDynamoDbAdminUtils remoteUtils = new AmazonDynamoDbAdminUtils(remoteDynamoDb);
    private static final String TABLE_PREFIX = "okToDelete-testBillingMode.";
    private String fullTableName;
    private static final ArrayList<String> testTables = new ArrayList<>();
    private static final Logger LOG = LoggerFactory.getLogger(AmazonDynamoDbAdminUtilsIt.class);

    @BeforeEach
    void beforeEach() {
        String tableName = String.valueOf(System.currentTimeMillis());
        fullTableName = TABLE_PREFIX + tableName;
        testTables.add(fullTableName);
    }

    private CreateTableRequest getTestCreateTableRequest(String tableName) {

        return new CreateTableRequest()
            .withTableName(tableName)
            .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S),
                new AttributeDefinition(RANGE_KEY_FIELD, S),
                new AttributeDefinition(INDEX_FIELD, S))
            .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                new KeySchemaElement(RANGE_KEY_FIELD, KeyType.RANGE))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testGsi")
                .withKeySchema(new KeySchemaElement(INDEX_FIELD, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
            .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testLsi")
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                    new KeySchemaElement(INDEX_FIELD, KeyType.RANGE))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL)));
    }

    @Test
    void createTableIfNotExistsButTableInUseForCreating() {
        remoteDynamoDb.createTable(getTestCreateTableRequest(fullTableName));
        remoteUtils.createTableIfNotExists(getTestCreateTableRequest(fullTableName), 10);
    }

    /* This tests against hosted DynamoDB to ensure tables encounter TableInUse exception
    (local create times are too short) */
    @Test
    void createTableIfNotExistsButTableInUseForOtherStatus() throws InterruptedException {
        remoteDynamoDb.createTable(getTestCreateTableRequest(fullTableName));
        TableUtils.waitUntilActive(remoteDynamoDb, fullTableName);

        Throwable e = null;
        try {
            remoteDynamoDb.deleteTable(fullTableName);
            remoteUtils.createTableIfNotExists(getTestCreateTableRequest(fullTableName), 10);
        } catch (Throwable ex) {
            e = ex;
        }
        assert (e instanceof ResourceInUseException);
    }

    @Test
    void deleteTableIfExistsButTableInUse() throws InterruptedException {
        remoteDynamoDb.createTable(getTestCreateTableRequest(fullTableName));

        // intentionally wait for table to be created
        Throwable e = null;
        try {
            Thread.sleep(1000);
            remoteUtils.deleteTableIfExists(fullTableName, 10, 15);
        } catch (Throwable ex) {
            e = ex;
        }
        assertTrue(e instanceof ResourceInUseException);
        TableUtils.waitUntilActive(remoteDynamoDb, fullTableName);
    }

    @AfterAll
    static void afterAll() throws InterruptedException {
        Thread.sleep(5000);
        for (String table : testTables) {
            try {
                remoteUtils.deleteTableIfExists(table, 10, 600);
            } catch (ResourceInUseException e) {
                if (remoteDynamoDb.describeTable(table) != null
                    && remoteDynamoDb.describeTable(table).getTable() != null
                    && remoteDynamoDb.describeTable(table).getTable().getTableStatus() != null
                    && remoteDynamoDb.describeTable(table).getTable().getTableStatus().equals(
                    TableStatus.DELETING.toString())) {
                    LOG.info("table delete already in progress for table: " + table);
                }
            } catch (ResourceNotFoundException e) {
                LOG.error("table not found: " + table);
            }
        }
    }
}