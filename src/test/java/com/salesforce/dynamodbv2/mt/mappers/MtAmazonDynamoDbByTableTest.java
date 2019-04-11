package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByTable.MtAmazonDynamoDbBuilder;
import com.salesforce.dynamodbv2.mt.util.DynamoDbTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MtAmazonDynamoDbByTableTest {

    private final AmazonDynamoDB localDynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
    private CreateTableRequest request;
    private MtAmazonDynamoDbByTable.MtAmazonDynamoDbBuilder mtDynamoDbByTableBuilder;
    private String fullTableName;

    private static final MtAmazonDynamoDbContextProvider ctx =
            new MtAmazonDynamoDbContextProviderThreadLocalImpl();
    private static final String ID_ATTR_NAME = "id";
    private static final String tablePrefix = "oktodelete-testBillingMode";

    @BeforeEach
    void beforeEach() {
        ctx.setContext("");
        String tableName = DynamoDbTestUtils.getTimestampTableName();
        fullTableName = DynamoDbTestUtils.getTableNameWithPrefix(tablePrefix, tableName, ".");

        request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement(ID_ATTR_NAME, HASH))
                .withAttributeDefinitions(
                        new AttributeDefinition(ID_ATTR_NAME, S));

        mtDynamoDbByTableBuilder = MtAmazonDynamoDbByTable.builder()
                .withTablePrefix(tablePrefix)
                .withAmazonDynamoDb(AmazonDynamoDbLocal.getAmazonDynamoDbLocal())
                .withContext(ctx);
    }

    @Test
    void testTableBuilderInterface() throws InterruptedException {
        TableBuilder tableBuilder = mtDynamoDbByTableBuilder;
        tableBuilder.withBillingMode(BillingMode.PROVISIONED);
        MtAmazonDynamoDbByTable mtDynamoDbByTable = ((MtAmazonDynamoDbBuilder) tableBuilder).build();

        mtDynamoDbByTable.createTable(request);
        TableUtils.waitUntilActive(localDynamoDb, fullTableName);
        DynamoDbTestUtils.assertProvisionedIsSet(fullTableName, localDynamoDb, 1L);
    }

    @Test
    void testMtAmazonDynamoDbByTableProvisionedIsSetWhenBillingModePassedIn() throws InterruptedException {
        mtDynamoDbByTableBuilder.withBillingMode(BillingMode.PROVISIONED);
        MtAmazonDynamoDbByTable mtDynamoDbByTable = mtDynamoDbByTableBuilder.build();

        mtDynamoDbByTable.createTable(request);
        TableUtils.waitUntilActive(localDynamoDb, fullTableName);
        DynamoDbTestUtils.assertProvisionedIsSet(fullTableName, localDynamoDb, 1L);
    }

    @Test
    void testMtAmazonDynamoDbByTableProvisionedIsSetWhenBillingModeAlreadySetOnRequest() throws InterruptedException {
        MtAmazonDynamoDbByTable mtDynamoDbByTable = mtDynamoDbByTableBuilder.build();
        request.withBillingMode(BillingMode.PROVISIONED);

        mtDynamoDbByTable.createTable(request);
        TableUtils.waitUntilActive(localDynamoDb, fullTableName);
        DynamoDbTestUtils.assertProvisionedIsSet(fullTableName, localDynamoDb, 1L);
    }

    @Test
    void testMtAmazonDynamoDbByTablePayPerRequestIsSetWhenBillingModePassedIn() throws InterruptedException {
        mtDynamoDbByTableBuilder.withBillingMode(BillingMode.PAY_PER_REQUEST);
        MtAmazonDynamoDbByTable mtDynamoDbByTable = mtDynamoDbByTableBuilder.build();

        mtDynamoDbByTable.createTable(request);
        TableUtils.waitUntilActive(localDynamoDb, fullTableName);
        DynamoDbTestUtils.assertPayPerRequestIsSet(fullTableName, localDynamoDb);
    }

    @Test
    void testMtAmazonDynamoDbByTablePayPerRequestIsSetWhenBillingModeAlreadySetOnRequest() throws InterruptedException {
        MtAmazonDynamoDbByTable mtDynamoDbByTable = mtDynamoDbByTableBuilder.build();
        request.withBillingMode(BillingMode.PAY_PER_REQUEST);

        mtDynamoDbByTable.createTable(request);
        TableUtils.waitUntilActive(localDynamoDb, fullTableName);
        DynamoDbTestUtils.assertPayPerRequestIsSet(fullTableName, localDynamoDb);
    }
}