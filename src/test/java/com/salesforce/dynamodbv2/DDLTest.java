package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
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
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.salesforce.dynamodbv2.TestArgumentSupplier.TestArgument;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * @author msgroi
 */
class DDLTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;

    @TestTemplate
    @ExtendWith(TestSetupInvocationContextProvider.class)
    void describeTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            assertEquals(TABLE1, testArgument.getAmazonDynamoDB().describeTable(TABLE1).getTable().getTableName());
        });
    }

    @TestTemplate
    @ExtendWith(TestSetupInvocationContextProvider.class)
    void createAndDeleteTable(TestArgument testArgument) {
        String org = testArgument.getOrgs().get(0);
        MT_CONTEXT.setContext(org);
        List<Map<String, AttributeValue>> items = testArgument.getAmazonDynamoDB()
            .scan(new ScanRequest().withTableName(TABLE1)).getItems(); // assert data is present
        assertTrue(items.size() > 0);
        new TestAmazonDynamoDbAdminUtils(testArgument.getAmazonDynamoDB())
            .deleteTableIfExists(TABLE1, getPollInterval(), TIMEOUT_SECONDS);
        try {
            testArgument.getAmazonDynamoDB().describeTable(TABLE1);
            fail("expected ResourceNotFoundException not encountered");
        } catch (ResourceNotFoundException ignore) {
        }
        new TestAmazonDynamoDbAdminUtils(testArgument.getAmazonDynamoDB())
            .createTableIfNotExists(new CreateTableRequest()
                .withTableName(TABLE1)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S))
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withStreamSpecification(new StreamSpecification()
                    .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                    .withStreamEnabled(true)), getPollInterval());
        items = testArgument.getAmazonDynamoDB() // assert no leftover data
            .scan(new ScanRequest().withTableName(TABLE1)).getItems();
        assertEquals(0, items.size());
    }

}