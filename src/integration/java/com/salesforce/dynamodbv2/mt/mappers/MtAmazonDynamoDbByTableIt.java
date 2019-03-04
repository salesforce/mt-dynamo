package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MtAmazonDynamoDbByTableIt {

    static final Regions REGION = Regions.US_EAST_1;
    private static AmazonDynamoDB remoteDynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .withRegion(REGION).build();
    private static AmazonDynamoDbAdminUtils remoteUtils = new AmazonDynamoDbAdminUtils(remoteDynamoDB);
    private static final String TABLE_PREFIX = "oktodelete-testBillingMode.";
    String tableName;
    String fullTableName;

    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";

    CreateTableRequest request;

    void assertPayPerRequestIsSet() {
        assertEquals(BillingMode.PAY_PER_REQUEST.toString(), remoteDynamoDB.describeTable(
                fullTableName).getTable().getBillingModeSummary().getBillingMode());
        assertNull(request.getProvisionedThroughput());
    }

    @BeforeEach
    void beforeEach() {
        tableName = new String(String.valueOf(System.currentTimeMillis()));
        fullTableName = TABLE_PREFIX + tableName;

        request = new CreateTableRequest()
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
    }

    @Test
    void testMtAmazonDynamoDbByTablePayPerRequestIsSetWhenBillingModePassedIn() throws InterruptedException {

        MtAmazonDynamoDbByTable mtDynamoDbByTable = MtAmazonDynamoDbByTable.builder()
                .withTablePrefix(TABLE_PREFIX)
                .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
                .withBillingMode(BillingMode.PAY_PER_REQUEST)
                .withContext(MT_CONTEXT)
                .build();

        mtDynamoDbByTable.createTable(request);

        TableUtils.waitUntilActive(remoteDynamoDB, fullTableName);
        assertPayPerRequestIsSet();
    }

    @Test
    void testMtAmazonDynamoDbByTablePayPerRequestIsSetWhenBillingModeAlreadySet() throws InterruptedException {

        request.withBillingMode(BillingMode.PAY_PER_REQUEST);

        MtAmazonDynamoDbByTable mtDynamoDbByTable = MtAmazonDynamoDbByTable.builder()
                .withTablePrefix(TABLE_PREFIX)
                .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
                .withContext(MT_CONTEXT)
                .build();

        mtDynamoDbByTable.createTable(request);

        TableUtils.waitUntilActive(remoteDynamoDB, fullTableName);
        assertPayPerRequestIsSet();
    }

    @AfterEach
    void afterEach() {
        remoteUtils.deleteTableIfExists(fullTableName, 10, 600);
    }


}
