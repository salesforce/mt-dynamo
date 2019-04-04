package com.salesforce.dynamodbv2.mt.admin;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AmazonDynamoDbAdminUtilsTest {

    AmazonDynamoDB localDynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    AmazonDynamoDbAdminUtils localUtils = new AmazonDynamoDbAdminUtils(localDynamoDb);
    private static final String TABLE_PREFIX = "oktodelete-testBillingMode.";
    String tableName;
    String fullTableName;

    @BeforeEach
    void beforeEach() {
        tableName = String.valueOf(System.currentTimeMillis());
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
    void createTableIfNotExistsIfTableDoesNotExist() throws InterruptedException {
        localUtils.createTableIfNotExists(getTestCreateTableRequest(fullTableName), 10);
        TableUtils.waitUntilActive(localDynamoDb, fullTableName);

        assert ((TableStatus.ACTIVE.toString()).equals(
                localDynamoDb.describeTable(fullTableName).getTable().getTableStatus()));
    }

    @Test
    void createTableIfNotExistsIfTableExistsWithDifferentDescription() throws InterruptedException {
        localDynamoDb.createTable(getTestCreateTableRequest(fullTableName));
        TableUtils.waitUntilActive(localDynamoDb, fullTableName);

        Throwable e = null;
        try {
            localUtils.createTableIfNotExists(new CreateTableRequest()
                    .withTableName(fullTableName)
                    .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S),
                            new AttributeDefinition(RANGE_KEY_FIELD, S))
                    .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                            new KeySchemaElement(RANGE_KEY_FIELD, KeyType.RANGE))
                    .withProvisionedThroughput(new ProvisionedThroughput(
                            1L, 1L)), 10);

        } catch (Throwable ex) {
            e = ex;
        }

        assert (e instanceof IllegalArgumentException);
    }

    @Test
    void createTableIfNotExistsIfTableExistsWithSameDescription() throws InterruptedException {
        localDynamoDb.createTable(getTestCreateTableRequest(fullTableName));
        TableUtils.waitUntilActive(localDynamoDb, fullTableName);

        Throwable e = null;
        try {
            localUtils.createTableIfNotExists(getTestCreateTableRequest(fullTableName),10);

        } catch (Throwable ex) {
            e = ex;
        }
        assertNull(e);
    }

    @Test
    void deleteTableIfExistsIfTableDoesNotExist() {
        String badTableName = "fake_table";
        localUtils.deleteTableIfExists(badTableName, 10, 10);

        Throwable e = null;
        try {
            localDynamoDb.describeTable("fake_table");
        } catch (Throwable ex) {
            e = ex;
        }
        assertTrue(e instanceof ResourceNotFoundException);
    }

    @Test
    void deleteTableIfExistsIfTableDoesExist() throws InterruptedException {
        localDynamoDb.createTable(getTestCreateTableRequest(fullTableName));
        TableUtils.waitUntilActive(localDynamoDb, fullTableName);
        localUtils.deleteTableIfExists(fullTableName, 10, 15);

        Throwable e = null;
        try {
            localDynamoDb.describeTable(fullTableName);
        } catch (Throwable ex) {
            e = ex;
        }
        assertTrue(e instanceof ResourceNotFoundException);
    }
}