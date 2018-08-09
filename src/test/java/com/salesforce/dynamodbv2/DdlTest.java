package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.TestSetup.TABLE1;
import static com.salesforce.dynamodbv2.TestSupport.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.TestSupport.TIMEOUT_SECONDS;
import static com.salesforce.dynamodbv2.TestSupport.getPollInterval;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.salesforce.dynamodbv2.TestArgumentSupplier.TestArgument;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests DDL operations.
 *
 * @author msgroi
 */
class DdlTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void describeTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            assertEquals(TABLE1, testArgument.getAmazonDynamoDb().describeTable(TABLE1).getTable().getTableName());
        });
    }

    @TestTemplate
    @ExtendWith(TestTemplateWithDataSetup.class)
    void createAndDeleteTable(TestArgument testArgument) {
        String org = testArgument.getOrgs().get(0);
        MT_CONTEXT.setContext(org);
        List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDb()
            .scan(new ScanRequest().withTableName(TABLE1)).getItems(); // assert data is present
        assertTrue(items.size() > 0);
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

}