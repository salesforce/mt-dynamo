package com.salesforce.dynamodbv2.testsupport;

import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_OTHER_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.IS_LOCAL_DYNAMO;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_HIGH_N_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_LOW_N_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_MIDDLE_N_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_OTHER_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_OTHER_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_OTHER_OTHER_FIELD_VALUE;

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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Performs default table creation and population logic.  To override table creation of data population logic,
 * provide your own TestSetup implementation.  To add more tables to the default set, extend this class, call
 * super.setupTest() from within your accept method.  See {@link DefaultArgumentProvider} for more details.
 *
 * @author msgroi
 */
public class DefaultTestSetup implements TestSetup {

    private static final MtAmazonDynamoDbContextProvider mtContext = ArgumentBuilder.MT_CONTEXT;
    public static final String TABLE1 = "Table1"; // has hk
    public static final String TABLE2 = "Table2"; // has hk
    public static final String TABLE3 = "Table3"; // has hk, rk(S)
    public static final String TABLE4 = "Table4"; // has hk, rk(N)
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

    @Override
    public void setupTableData(AmazonDynamoDB amazonDynamoDb,
        ScalarAttributeType hashKeyAttrType,
        String org,
        CreateTableRequest createTableRequest) {
        String table = createTableRequest.getTableName();
        final Optional<ScalarAttributeType> rangeKeyAttributeTypeOpt = getScalarAttributeType(createTableRequest,
            KeyType.RANGE);
        if (!rangeKeyAttributeTypeOpt.isPresent()) { // no range key
            // (hk-only tables) add two rows:
            // (hk1, {no rk}, table-and-org-specific-field-value1)
            // (hk2, {no rk}, table-and-org-specific-field-value2)
            amazonDynamoDb.putItem(
                    new PutItemRequest().withTableName(table)
                            .withItem(ItemBuilder.builder(hashKeyAttrType, HASH_KEY_VALUE)
                                    .someField(S, SOME_FIELD_VALUE + table + org)
                                    .build()));
            amazonDynamoDb.putItem(
                    new PutItemRequest().withTableName(table)
                            .withItem(ItemBuilder.builder(hashKeyAttrType, HASH_KEY_OTHER_VALUE)
                                    .someField(S, SOME_OTHER_OTHER_FIELD_VALUE + table + org)
                                    .build()));
        } else { // there is a range key
            // (hk-rk tables) add two rows:
            // (hk1, rk1, table-and-org-specific-field-value1)
            // (hk1, rk2, table-and-org-specific-field-value2, index-field-value)

            switch (rangeKeyAttributeTypeOpt.get()) {
                case S:
                    amazonDynamoDb.putItem(
                        new PutItemRequest().withTableName(table)
                            .withItem(ItemBuilder.builder(hashKeyAttrType, HASH_KEY_VALUE)
                                .someField(S, SOME_FIELD_VALUE + table + org)
                                .rangeKey(S, TestSupport.RANGE_KEY_S_VALUE)
                                .build()));
                    amazonDynamoDb.putItem(
                        new PutItemRequest().withTableName(table)
                            .withItem(ItemBuilder.builder(hashKeyAttrType, HASH_KEY_VALUE)
                                .someField(S, SOME_OTHER_FIELD_VALUE + table + org)
                                .rangeKey(S, RANGE_KEY_OTHER_S_VALUE)
                                .indexField(S, INDEX_FIELD_VALUE)
                                .build()));
                    break;
                case N:
                    amazonDynamoDb.putItem(
                        new PutItemRequest().withTableName(table)
                            .withItem(ItemBuilder.builder(hashKeyAttrType, HASH_KEY_VALUE)
                                .someField(S, SOME_FIELD_VALUE + table + org)
                                .rangeKey(N, RANGE_KEY_LOW_N_VALUE)
                                .build()));
                    amazonDynamoDb.putItem(
                        new PutItemRequest().withTableName(table)
                            .withItem(ItemBuilder.builder(hashKeyAttrType, HASH_KEY_VALUE)
                                .someField(S, SOME_OTHER_FIELD_VALUE + table + org)
                                .rangeKey(N, RANGE_KEY_MIDDLE_N_VALUE)
                                .build()));
                    amazonDynamoDb.putItem(
                        new PutItemRequest().withTableName(table)
                            .withItem(ItemBuilder.builder(hashKeyAttrType, HASH_KEY_VALUE)
                                .someField(S, SOME_OTHER_FIELD_VALUE + table + org)
                                .rangeKey(N, RANGE_KEY_HIGH_N_VALUE)
                                .build()));
                    break;
                default:
                    throw new IllegalArgumentException("unsupported type " + rangeKeyAttributeTypeOpt.get()
                        + " encountered");
            }
        }
    }

    /**
     * Gets the {@code ScalarAttributeType} associated with the one key in the key schema with {@code KeyType}
     * {@code keyType}.
     */
    private Optional<ScalarAttributeType> getScalarAttributeType(CreateTableRequest createTableRequest,
        KeyType keyType) {
        Optional<ScalarAttributeType> retval;
        final Set<String> setWithKeyAttributeNameIfItExists = createTableRequest.getKeySchema().stream()
            .filter(keySchemaElement -> keyType.equals(KeyType.fromValue(keySchemaElement.getKeyType())))
            .map(KeySchemaElement::getAttributeName)
            .collect(Collectors.toSet());
        if (setWithKeyAttributeNameIfItExists.isEmpty()) {
            retval = Optional.empty();
        } else {
            Preconditions.checkState(setWithKeyAttributeNameIfItExists.size() == 1,
                "There should be only 1 " + keyType + " key but there are "
                    + setWithKeyAttributeNameIfItExists.size());
            final String keyAttributeName = Iterables.getOnlyElement(setWithKeyAttributeNameIfItExists);
            final Set<ScalarAttributeType> setWithKeyAttributeType = createTableRequest.getAttributeDefinitions()
                .stream()
                .filter(attributeDefinition -> attributeDefinition.getAttributeName().equals(keyAttributeName))
                .map(AttributeDefinition::getAttributeType)
                .map(ScalarAttributeType::fromValue)
                .collect(Collectors.toSet());
            Preconditions.checkState(setWithKeyAttributeType.size() == 1,
                "There should be exactly 1 attributeType associated with the " + keyType + " key named "
                    + keyAttributeName + ", but there are " + setWithKeyAttributeType.size());
            retval = Optional.of(Iterables.getOnlyElement(setWithKeyAttributeType));
        }

        return retval;
    }

    private List<CreateTableRequest> getCreateRequests(ScalarAttributeType hashKeyAttrType) {
        return ImmutableList.of(
            new CreateTableRequest()
                .withTableName(TABLE1)
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, hashKeyAttrType))
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)),
            new CreateTableRequest()
                .withTableName(TABLE2)
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, hashKeyAttrType))
                .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)),
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
                    .withProjection(new Projection().withProjectionType(ProjectionType.ALL))),
            new CreateTableRequest()
                .withTableName(TABLE4)
                .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, hashKeyAttrType),
                    new AttributeDefinition(RANGE_KEY_FIELD, N), // unlike TABLE3
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