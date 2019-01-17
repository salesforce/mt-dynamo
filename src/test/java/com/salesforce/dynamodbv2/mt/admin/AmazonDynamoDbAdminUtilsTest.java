package com.salesforce.dynamodbv2.mt.admin;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static org.junit.jupiter.api.Assertions.*;

class AmazonDynamoDbAdminUtilsTest {

    static final Regions REGION = Regions.US_EAST_1;
    AmazonDynamoDB remoteDynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .withRegion(REGION).build();

    AmazonDynamoDbAdminUtils remoteUtils = new AmazonDynamoDbAdminUtils(remoteDynamoDB);

    AmazonDynamoDB localDynamoDB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    AmazonDynamoDbAdminUtils localUtils = new AmazonDynamoDbAdminUtils(localDynamoDB);
    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";
    String TABLE_PREFIX = "oktodelete-testBillingMode.";
    String table_name;
    String full_table_name;

    @BeforeEach
    void beforeEach(){
        table_name = new String(String.valueOf(System.currentTimeMillis()));
        full_table_name = TABLE_PREFIX + table_name;
    }

    @Test
    void createTableIfNotExists() {
    }

    private CreateTableRequest getTestCreateTableRequest(String tableName){

        return new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S),
                        new AttributeDefinition(RANGE_KEY_FIELD, S),
                        new AttributeDefinition(INDEX_FIELD, S))
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                        new KeySchemaElement(RANGE_KEY_FIELD, KeyType.RANGE))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                        .withKeySchema(new KeySchemaElement(INDEX_FIELD, KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                        .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                                new KeySchemaElement(INDEX_FIELD, KeyType.RANGE))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)));
    }

    @Test
    void createTableIfNotExistsIfTableDoesNotExist() throws InterruptedException {
        localUtils.createTableIfNotExists(getTestCreateTableRequest(full_table_name), 10);
        TableUtils.waitUntilActive(localDynamoDB, full_table_name);
        assert((TableStatus.ACTIVE.toString()).equals(localDynamoDB.describeTable(full_table_name).getTable().getTableStatus()));
    }

    @Test
    void createTableIfNotExistsIfTableExistsWithDifferentDescription() throws InterruptedException {
        localDynamoDB.createTable(getTestCreateTableRequest(full_table_name));
        TableUtils.waitUntilActive(localDynamoDB, full_table_name);

        Throwable e = null;
        try {
            localUtils.createTableIfNotExists(new CreateTableRequest()
                    .withTableName(full_table_name)
                    .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S),
                            new AttributeDefinition(RANGE_KEY_FIELD, S))
                    .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                            new KeySchemaElement(RANGE_KEY_FIELD, KeyType.RANGE))
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)), 10);

        } catch (Throwable ex) {
            e = ex;
        }
        assert(e instanceof IllegalArgumentException);
    }

    @Test
    void createTableIfNotExistsIfTableExistsWithSameDescription() throws InterruptedException {
        localDynamoDB.createTable(getTestCreateTableRequest(full_table_name));
        TableUtils.waitUntilActive(localDynamoDB, full_table_name);

        Throwable e = null;
        try {
            localUtils.createTableIfNotExists(getTestCreateTableRequest(full_table_name),10);

        } catch (Throwable ex) {
            e = ex;
        }
        assertNull(e);
    }

    /* This tests against hosted DynamoDB to ensure tables encounter TableInUse exception (local create times are too short) */
    @Test
    void createTableIfNotExistsButTableInUseForCreating() {

        remoteDynamoDB.createTable(getTestCreateTableRequest(full_table_name));
        remoteUtils.createTableIfNotExists(getTestCreateTableRequest(full_table_name), 10);

        // cleanup
        remoteUtils.deleteTableIfExists(full_table_name, 10, 600);
    }

    /* This tests against hosted DynamoDB to ensure tables encounter TableInUse exception (local create times are too short) */
    @Test
    void createTableIfNotExistsButTableInUseForOtherStatus() throws InterruptedException {

        remoteDynamoDB.createTable(getTestCreateTableRequest(full_table_name));
        TableUtils.waitUntilActive(remoteDynamoDB, full_table_name);

        Throwable e = null;
        try {
            remoteDynamoDB.deleteTable(full_table_name);
            remoteUtils.createTableIfNotExists(getTestCreateTableRequest(full_table_name), 10);
        } catch (Throwable ex) {
            e = ex;
        }
        assert(e instanceof ResourceInUseException);
    }

    @Test
    void deleteTableIfExistsIfTableDoesNotExist() {
        String badTableName = "fake_table";
        localUtils.deleteTableIfExists(badTableName, 10, 10);

        Throwable e = null;
        try {
            localDynamoDB.describeTable("fake_table");
        } catch (Throwable ex) {
            e = ex;
        }
        assertTrue(e instanceof ResourceNotFoundException);
    }

    @Test
    void deleteTableIfExistsIfTableDoesExist() throws InterruptedException {
        localDynamoDB.createTable(getTestCreateTableRequest(full_table_name));
        TableUtils.waitUntilActive(localDynamoDB, full_table_name);
        localUtils.deleteTableIfExists(full_table_name, 10, 15);

        Throwable e = null;
        try {
            localDynamoDB.describeTable(full_table_name);
        } catch (Throwable ex) {
            e = ex;
        }
        assertTrue(e instanceof ResourceNotFoundException);
    }

    /* This tests against hosted DynamoDB to ensure tables encounter TableInUse exception (local create times are too short) */
    @Test
    void deleteTableIfExistsButTableInUse() throws InterruptedException {
        remoteDynamoDB.createTable(getTestCreateTableRequest(full_table_name));

        // intentionally wait for table to be created
        Throwable e = null;
        try {
            Thread.sleep(1000);
            remoteUtils.deleteTableIfExists(full_table_name, 10, 15);
        } catch (Throwable ex) {
            e = ex;
        }
        assertTrue(e instanceof ResourceInUseException);

        // cleanup
        TableUtils.waitUntilActive(remoteDynamoDB, full_table_name);
        remoteUtils.deleteTableIfExists(full_table_name, 10, 600);
    }
}