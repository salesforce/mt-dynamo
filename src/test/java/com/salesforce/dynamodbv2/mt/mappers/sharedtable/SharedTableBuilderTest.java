package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SharedTableBuilderTest {

    static final Regions REGION = Regions.US_EAST_1;
    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";

    AmazonDynamoDB localDynamoDB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

    private static final String TABLE_PREFIX = "oktodelete-testBillingMode.";
    String tableName;
    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
            new MtAmazonDynamoDbContextProviderThreadLocalImpl();

    @BeforeEach
    void beforeEach() {
        tableName = new String(String.valueOf(System.currentTimeMillis()));
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
                .withDefaultProvisionedThroughput(1)
                .withCreateTableRequests(request)
                .withStreamsEnabled(false)
                .withPrecreateTables(true)
                .withTablePrefix(TABLE_PREFIX)
                .withAmazonDynamoDb(localDynamoDB)
                .withContext(MT_CONTEXT)
                .build();

        assertEquals(1, localDynamoDB.describeTable(TABLE_PREFIX + tableName)
                .getTable().getProvisionedThroughput().getWriteCapacityUnits().intValue());
        assertEquals(1, localDynamoDB.describeTable(TABLE_PREFIX + tableName)
                .getTable().getProvisionedThroughput().getReadCapacityUnits().intValue());
    }

    @Test
    void testBillingModeProvisionedThroughputIsSetForDefaultCreateTableRequests() {
        MtAmazonDynamoDbBySharedTable mtDynamoDb = SharedTableBuilder.builder()
                .withDefaultProvisionedThroughput(1)
                .withAmazonDynamoDb(localDynamoDB)
                .withTablePrefix(TABLE_PREFIX)
                .withPrecreateTables(true)
                .withContext(MT_CONTEXT)
                .build();

        assertEquals(1, localDynamoDB.describeTable(TABLE_PREFIX + "mt_sharedtablestatic_s_s")
                .getTable().getProvisionedThroughput().getWriteCapacityUnits().intValue());
        assertEquals(1, localDynamoDB.describeTable(TABLE_PREFIX + "mt_sharedtablestatic_s_s")
                .getTable().getProvisionedThroughput().getReadCapacityUnits().intValue());
    }
}