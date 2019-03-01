package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MtAmazonDynamoDbByTableTest {


    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";

    AmazonDynamoDB localDynamoDB = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

    private static final String tablePrefix = "oktodelete-testBillingMode.";
    String tableName;
    String fullTableName;
    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
            new MtAmazonDynamoDbContextProviderThreadLocalImpl();

    CreateTableRequest request;

    @BeforeEach
    void beforeEach() {
        tableName = new String(String.valueOf(System.currentTimeMillis()));
        fullTableName = tablePrefix + tableName;

        request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement(ID_ATTR_NAME, HASH))
                .withAttributeDefinitions(
                        new AttributeDefinition(ID_ATTR_NAME, S));
    }

    @Test
    void testMtAmazonDynamoDbByTableProvisionedIsSetWhenBillingModeAlreadySet() throws InterruptedException {

        request.withBillingMode(BillingMode.PROVISIONED);

        MtAmazonDynamoDbByTable mtDynamoDbByTable = MtAmazonDynamoDbByTable.builder()
                .withTablePrefix(tablePrefix)
                .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
                .withContext(MT_CONTEXT)
                .build();

        mtDynamoDbByTable.createTable(request);

        TableUtils.waitUntilActive(localDynamoDB, fullTableName);

        assertNotEquals(BillingMode.PROVISIONED.toString(), request.getBillingMode());
        assertNotNull(request.getProvisionedThroughput());
        assert (request.getProvisionedThroughput().getReadCapacityUnits().equals(1L));
        assert (request.getProvisionedThroughput().getWriteCapacityUnits().equals(1L));
    }

}
