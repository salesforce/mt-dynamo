package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MtAmazonDynamoDbByTableTest {


    private static final String ID_ATTR_NAME = "id";
    //private static final String INDEX_ID_ATTR_NAME = "indexId";

    AmazonDynamoDB localDynamoDB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

    private static final String tablePrefix = "oktodelete-testBillingMode";
    String tableName;
    String fullTableName;
    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
            new MtAmazonDynamoDbContextProviderThreadLocalImpl();

    CreateTableRequest request;
    MtAmazonDynamoDbByTable.MtAmazonDynamoDbBuilder mtDynamoDbByTableBuilder;

    void assertPayPerRequestIsSet() {
        assertEquals(BillingMode.PAY_PER_REQUEST.toString(), localDynamoDB.describeTable(
                tableName).getTable().getBillingModeSummary().getBillingMode());
        assertNull(request.getProvisionedThroughput());
    }

    @BeforeEach
    void beforeEach() {
        tableName = new String(String.valueOf(System.currentTimeMillis()));
        fullTableName = tablePrefix + tableName;

        request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement(ID_ATTR_NAME, HASH))
                .withAttributeDefinitions(
                        new AttributeDefinition(ID_ATTR_NAME, S));

        mtDynamoDbByTableBuilder = MtAmazonDynamoDbByTable.builder()
                .withTablePrefix(tablePrefix)
                .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
                .withContext(MT_CONTEXT);
    }

    @Test
    void testMtAmazonDynamoDbByTableProvisionedIsSetWhenBillingModeAlreadySet() throws InterruptedException {

        request.withBillingMode(BillingMode.PROVISIONED);
        request.withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));

        MtAmazonDynamoDbByTable mtDynamoDbByTable = mtDynamoDbByTableBuilder.build();

        mtDynamoDbByTable.createTable(request);
        TableUtils.waitUntilActive(localDynamoDB, fullTableName);

        assertEquals(BillingMode.PROVISIONED.toString(), localDynamoDB.describeTable(
                fullTableName).getTable().getBillingModeSummary().getBillingMode());
//        assertNotNull(request.getProvisionedThroughput());
//        assert (request.getProvisionedThroughput().getReadCapacityUnits().equals(1L));
//        assert (request.getProvisionedThroughput().getWriteCapacityUnits().equals(1L));
    }

    @Test
    void test2() {
        mtDynamoDbByTableBuilder.withBillingMode(BillingMode.PAY_PER_REQUEST);
        MtAmazonDynamoDbByTable mtDynamoDbByTable = mtDynamoDbByTableBuilder.build();


        mtDynamoDbByTable.createTable(request);
        assertPayPerRequestIsSet();

    }

    @Test
    void test3() {
        MtAmazonDynamoDbByTable mtDynamoDbByTable = mtDynamoDbByTableBuilder.build();

        request.withBillingMode(BillingMode.PAY_PER_REQUEST);

        mtDynamoDbByTable.createTable(request);
        assertPayPerRequestIsSet();
    }
}