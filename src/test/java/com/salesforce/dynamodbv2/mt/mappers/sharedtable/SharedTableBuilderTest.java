package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.*;

class SharedTableBuilderTest {

    /**
     * Some of these tests run against actual DynamoDB (remote), due to limitations in the LocalDynamoDBClient that don't allow
     * BillingMode PAY_PER_REQUEST to be enabled without specifying throughput (which is what the API allows).
     */

    static final Regions REGION = Regions.US_EAST_1;
    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";

    AmazonDynamoDB remoteDynamoDB = AmazonDynamoDBClientBuilder.standard()
        .withCredentials(new EnvironmentVariableCredentialsProvider())
            .withRegion(REGION).build();

    AmazonDynamoDB localDynamoDB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

    String TABLE_PREFIX = "oktodelete-testBillingMode.";
    private MtAmazonDynamoDbContextProvider mtContext;
    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
            new MtAmazonDynamoDbContextProviderThreadLocalImpl();

    @Test
    void testBillingModePayPerRequestWithCustomTableRequest(){

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(new String(String.valueOf(System.currentTimeMillis())))
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
                .withDefaultProvisionedThroughput(0)
                .withCreateTableRequests(request)
                .withStreamsEnabled(false)
                .withPrecreateTables(true)
                .withTablePrefix(TABLE_PREFIX)
                .withAmazonDynamoDb(remoteDynamoDB)
                .withContext(MT_CONTEXT)
                .build();

        assertEquals(BillingMode.PAY_PER_REQUEST.toString(), remoteDynamoDB.describeTable(TABLE_PREFIX + "_sample")
                .getTable().getBillingModeSummary().getBillingMode());
    }

    @Test
    void testBillingModePayPerRequestWithDefaults(){

        SharedTableBuilder.builder()
                .withDefaultProvisionedThroughput(0)
                .withStreamsEnabled(false)
                .withPrecreateTables(true)
                .withTablePrefix(TABLE_PREFIX)
                .withAmazonDynamoDb(remoteDynamoDB)
                .withContext(MT_CONTEXT)
                .build();

        assertEquals(BillingMode.PAY_PER_REQUEST.toString(), remoteDynamoDB.describeTable(TABLE_PREFIX + "mt_sharedtablestatic_s_s")
                .getTable().getBillingModeSummary().getBillingMode());

    }

    @Test
    void testBillingModeProvisioned() {
        MtAmazonDynamoDbBySharedTable mtDynamoDb = SharedTableBuilder.builder()
                .withDefaultProvisionedThroughput(1)
                .withAmazonDynamoDb(localDynamoDB)
                .withTablePrefix(TABLE_PREFIX)
                .withPrecreateTables(true)
                .withContext(MT_CONTEXT)
                .build();

                assertEquals(1, localDynamoDB.describeTable(TABLE_PREFIX + "mt_sharedtablestatic_s_s")
                        .getTable().getProvisionedThroughput().getWriteCapacityUnits().intValue());
                assertEquals(1, localDynamoDB.describeTable(TABLE_PREFIX + "mt_sharedtablestatic_s_n")
                        .getTable().getProvisionedThroughput().getReadCapacityUnits().intValue());
    }
}