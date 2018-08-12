package com.salesforce.dynamodbv2.testsupport;

import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.IS_LOCAL_DYNAMO;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithValues;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import java.util.List;
import java.util.Optional;

/**
 * Performs default table creation and population logic.  To override table creation of data population logic,
 * provide your own TestSetup implementation.  To add more tables to the default set, extend this class, call
 * super.setupTest() from within your accept method.  See {@link DefaultArgumentProvider} for more details.
 *
 * @author msgroi
 */
public class DefaultTestSetup implements TestSetup {

    private static final MtAmazonDynamoDbContextProvider mtContext = ArgumentBuilder.MT_CONTEXT;
    public static final String TABLE1 = "Table1";
    public static final String TABLE2 = "Table2";
    public static final String TABLE3 = "Table3";
    private List<CreateTableRequest> createTableRequests;

    @Override
    public void setupTest(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            mtContext.setContext(org);
            createTableRequests = getCreateRequests(testArgument.getHashKeyAttrType());
            createTableRequests.forEach(createTableRequest -> {
                new TestAmazonDynamoDbAdminUtils(testArgument.getAmazonDynamoDb())
                    .createTableIfNotExists(createTableRequest, getPollInterval());
                setupTableData(testArgument.getAmazonDynamoDb(),
                               testArgument.getHashKeyAttrType(),
                               org,
                               createTableRequest);
            });
        });
    }

    @SuppressWarnings("checkstyle:Indentation")
    @Override
    public void setupTableData(AmazonDynamoDB amazonDynamoDb,
        ScalarAttributeType hashKeyAttrType,
        String org,
        CreateTableRequest createTableRequest) {
        String table = createTableRequest.getTableName();
        boolean hasRangeKey = createTableRequest.getKeySchema().stream().anyMatch(
                keySchemaElement -> KeyType.valueOf(keySchemaElement.getKeyType()) == RANGE);
        if (!hasRangeKey) {
            // hk-only tables
            amazonDynamoDb.putItem(
                new PutItemRequest().withTableName(table)
                    .withItem(buildItemWithSomeFieldValue(hashKeyAttrType,
                        SOME_FIELD_VALUE + table + org)));
        } else {
            // hk-rk tables
            amazonDynamoDb.putItem(
                new PutItemRequest().withTableName(table)
                    .withItem(buildHkRkItemWithSomeFieldValue(hashKeyAttrType,
                        SOME_FIELD_VALUE + table + org)));
            amazonDynamoDb.putItem(
                new PutItemRequest().withTableName(table)
                    .withItem(buildItemWithValues(hashKeyAttrType, HASH_KEY_VALUE,
                        Optional.of(RANGE_KEY_VALUE + "2"),
                        SOME_FIELD_VALUE + table + org + "2",
                        Optional.of(INDEX_FIELD_VALUE))));
        }
    }

    private List<CreateTableRequest> getCreateRequests(ScalarAttributeType hashKeyAttrType) {
        return ImmutableList.of(
            new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, hashKeyAttrType))
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withTableName(TABLE1),
            new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, hashKeyAttrType))
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withTableName(TABLE2),
            new CreateTableRequest()
                .withTableName(TABLE3)
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, hashKeyAttrType),
                    new AttributeDefinition(RANGE_KEY_FIELD, S),
                    new AttributeDefinition(INDEX_FIELD, S))
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                    new KeySchemaElement(RANGE_KEY_FIELD, RANGE))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                    .withKeySchema(new KeySchemaElement(INDEX_FIELD, KeyType.HASH))
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                    .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                        new KeySchemaElement(INDEX_FIELD, RANGE))
                    .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
        );
    }

    private int getPollInterval() {
        return IS_LOCAL_DYNAMO ? 0 : 1;
    }

}