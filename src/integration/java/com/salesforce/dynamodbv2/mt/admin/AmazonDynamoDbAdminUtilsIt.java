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
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AmazonDynamoDbAdminUtilsIt {

    /* This tests against hosted DynamoDB to ensure tables encounter TableInUse exception
    (local create times are too short) */

    static final Regions REGION = Regions.US_EAST_1;
    AmazonDynamoDB remoteDynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .withRegion(REGION).build();

    AmazonDynamoDbAdminUtils remoteUtils = new AmazonDynamoDbAdminUtils(remoteDynamoDB);

    AmazonDynamoDB localDynamoDB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    AmazonDynamoDbAdminUtils localUtils = new AmazonDynamoDbAdminUtils(localDynamoDB);
    private static final String TABLE_PREFIX = "oktodelete-testBillingMode.";
    String tableName;
    String fullTableName;

    @BeforeEach
    void beforeEach() {
        tableName = new String(String.valueOf(System.currentTimeMillis()));
        fullTableName = TABLE_PREFIX + tableName;
    }

    @Test
    void createTableIfNotExists() {
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
    void createTableIfNotExistsButTableInUseForCreating() {

        remoteDynamoDB.createTable(getTestCreateTableRequest(fullTableName));
        remoteUtils.createTableIfNotExists(getTestCreateTableRequest(fullTableName), 10);

        // cleanup
        remoteUtils.deleteTableIfExists(fullTableName, 10, 600);
    }

    /* This tests against hosted DynamoDB to ensure tables encounter TableInUse exception
    (local create times are too short) */
    @Test
    void createTableIfNotExistsButTableInUseForOtherStatus() throws InterruptedException {

        remoteDynamoDB.createTable(getTestCreateTableRequest(fullTableName));
        TableUtils.waitUntilActive(remoteDynamoDB, fullTableName);

        Throwable e = null;
        try {
            remoteDynamoDB.deleteTable(fullTableName);
            remoteUtils.createTableIfNotExists(getTestCreateTableRequest(fullTableName), 10);
        } catch (Throwable ex) {
            e = ex;
        }
        assert (e instanceof ResourceInUseException);
    }

    @Test
    void deleteTableIfExistsButTableInUse() throws InterruptedException {
        remoteDynamoDB.createTable(getTestCreateTableRequest(fullTableName));

        // intentionally wait for table to be created
        Throwable e = null;
        try {
            Thread.sleep(1000);
            remoteUtils.deleteTableIfExists(fullTableName, 10, 15);
        } catch (Throwable ex) {
            e = ex;
        }
        assertTrue(e instanceof ResourceInUseException);

        // cleanup
        TableUtils.waitUntilActive(remoteDynamoDB, fullTableName);
        remoteUtils.deleteTableIfExists(fullTableName, 10, 600);
    }
}