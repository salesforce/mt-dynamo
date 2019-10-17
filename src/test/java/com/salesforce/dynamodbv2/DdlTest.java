package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.TIMEOUT_SECONDS;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getPollInterval;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider.DefaultArgumentProviderConfig;
import com.salesforce.dynamodbv2.testsupport.TestAmazonDynamoDbAdminUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests DDL operations.
 *
 * @author msgroi
 */
class DdlTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = ArgumentBuilder.MT_CONTEXT;

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void describeTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(org
            -> assertEquals(TABLE1, testArgument.getAmazonDynamoDb().describeTable(TABLE1).getTable().getTableName()));
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1})
    void createAndDeleteTable(TestArgument testArgument) {
        String org = testArgument.getOrgs().get(0);
        MT_CONTEXT.setContext(org);
        List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb()
            .scan(new ScanRequest().withTableName(TABLE1)).getItems(); // assert data is present
        assertFalse(items.isEmpty());
        new TestAmazonDynamoDbAdminUtils(testArgument.getAmazonDynamoDb())
            .deleteTableIfExists(TABLE1, getPollInterval(), TIMEOUT_SECONDS);
        try {
            testArgument.getAmazonDynamoDb().describeTable(TABLE1);
            fail("expected ResourceNotFoundException not encountered");
        } catch (ResourceNotFoundException ignore) {
            // expected
        }
        new TestAmazonDynamoDbAdminUtils(testArgument.getAmazonDynamoDb())
            .createTableIfNotExists(new CreateTableRequest()
                .withTableName(TABLE1)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, testArgument.getHashKeyAttrType()))
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH)), getPollInterval());
        items = testArgument.getAmazonDynamoDb() // assert no leftover data
            .scan(new ScanRequest().withTableName(TABLE1)).getItems();
        assertEquals(0, items.size());
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void createTableResponse(TestArgument testArgument) {
        String orgId = testArgument.getOrgs().get(0);
        MT_CONTEXT.withContext(orgId, () -> {
            String tableName = DdlTest.class.getSimpleName() + "." + "createTableResponse";
            CreateTableResult result = testArgument.getAmazonDynamoDb().createTable(new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(new AttributeDefinition("id", ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withStreamSpecification(new StreamSpecification()
                    .withStreamEnabled(true)
                    .withStreamViewType(StreamViewType.KEYS_ONLY)
                )
            );
            try {
                assertEquals(tableName, result.getTableDescription().getTableName());
                assertTrue(result.getTableDescription().getLatestStreamArn().contains(orgId));
            } finally {
                TableUtils.deleteTableIfExists(testArgument.getAmazonDynamoDb(), new DeleteTableRequest(tableName));
            }
        });
    }
}