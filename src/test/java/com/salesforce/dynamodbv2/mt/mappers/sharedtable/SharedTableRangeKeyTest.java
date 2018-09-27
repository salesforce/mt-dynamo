package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getPollInterval;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import com.salesforce.dynamodbv2.testsupport.TestAmazonDynamoDbAdminUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests whether it's possible to use a table with a KeySchema that has both a HK and RK for storing data
 * for virtual tables that have only a HK by not passing in a RK attribute or a null RK attribute value.
 *
 * @author msgroi
 */
class SharedTableRangeKeyTest {

    private static final String TABLENAME_PREFIX = "Table";

    @ParameterizedTest
    @EnumSource(ScalarAttributeType.class)
    void test(ScalarAttributeType scalarAttributeType) {
        AmazonDynamoDB amazonDynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

        // table name
        String tableName = TABLENAME_PREFIX + scalarAttributeType.name();

        // create table with hash key and range key of type string
        new TestAmazonDynamoDbAdminUtils(amazonDynamoDb)
            .createTableIfNotExists(new CreateTableRequest()
                .withTableName(tableName)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withAttributeDefinitions(
                    new AttributeDefinition(HASH_KEY_FIELD, S),
                    new AttributeDefinition(RANGE_KEY_FIELD, scalarAttributeType))
                .withKeySchema(
                    new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                    new KeySchemaElement(RANGE_KEY_FIELD, KeyType.RANGE)), getPollInterval());

        // insert an item that has no range key attribute
        try {
            amazonDynamoDb.putItem(new PutItemRequest().withTableName(tableName)
                .withItem(ItemBuilder.builder(S, "hk").build()));
            fail("expected exception not encountered");
        } catch (AmazonServiceException e) {
            assertEquals("One of the required keys was not given a value (Service: null; Status Code: 400; Error "
                + "Code: ValidationException; Request ID: null)", e.getMessage());
        }

        // insert an item that has a range key attribute with a null value
        try {
            amazonDynamoDb.putItem(new PutItemRequest().withTableName(tableName)
                .withItem(ItemBuilder.builder(S, "hk").rangeKey(S, null).build()));
            fail("expected exception not encountered");
        } catch (AmazonServiceException e) {
            assertEquals("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes "
                    + "(Service: null; Status Code: 400; Error Code: ValidationException; Request ID: null)",
                e.getMessage());
        }

        // insert an item that has a range key attribute with a null value
        try {
            amazonDynamoDb.putItem(new PutItemRequest().withTableName(tableName)
                .withItem(ItemBuilder.builder(S, "hk").rangeKey(S, "").build()));
            fail("expected exception not encountered");
        } catch (AmazonServiceException e) {
            assertEquals("One or more parameter values were invalid: An AttributeValue may not contain an "
                + "empty string (Service: null; Status Code: 400; Error Code: ValidationException; Request ID: null)",
                e.getMessage());
        }
    }

}