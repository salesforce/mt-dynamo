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
import static com.salesforce.dynamodbv2.testsupport.TestSupport.TIMEOUT_SECONDS;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithValues;

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
import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier.TestArgument;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * // TODO msgroi fix this if I end up merging table and data set up
 * Performs default table creation and population logic.  To override table creation of data population logic, call
 * withTableSetup() and/or withDataSetup() respectively.  To add more tables to the default set or add more data to the
 * default set of data, the pass implementations to those methods that themselves extend DefaultTableSetup and/or
 * DefaultDataSetup respectively.
 *
 * @author msgroi
 */
public class TestSetup { // TODO msgroi make this implement Consumer

    private static final MtAmazonDynamoDbContextProvider mtContext = TestArgumentSupplier.MT_CONTEXT;
    public static final String TABLE1 = "Table1";
    public static final String TABLE2 = "Table2";
    public static final String TABLE3 = "Table3";
    private TableSetup tableSetup = new DefaultTableSetup(); // TODO msgroi combine data and table setup
    private DataSetup dataSetup = new DefaultDataSetup(tableSetup);

    public TestSetup withTableSetup(TableSetup tableSetup) {
        this.tableSetup = tableSetup;
        return this;
    }

    public TestSetup withDataSetup(DataSetup dataSetup) {
        this.dataSetup = dataSetup;
        return this;
    }

    /**
     * Returns the class that's responsible for setting up both tables and data for a test.
     */
    public DataSetup getSetup() {
        return testArgument -> {
            tableSetup.accept(testArgument);
            dataSetup.accept(testArgument);
        };
    }

    public void teardown() {
        dataSetup.teardown();
        tableSetup.teardown();
    }

    public Consumer<TestArgument> getTeardown() {
        return testArgument -> teardown();
    }

    public interface TableSetup extends Consumer<TestArgument> {
        List<CreateTableRequest> getCreateTableRequests();

        default void teardown() {}
    }

    private class DefaultTableSetup implements TableSetup {

        List<CreateTableRequest> createTableRequests;
        List<TestArgument> testArguments = new ArrayList<>();

        @Override
        public void accept(TestArgument testArgument) {
            testArgument.getOrgs().forEach(org -> {
                mtContext.setContext(org);
                createTableRequests = getCreateRequests(testArgument.getHashKeyAttrType());
                createTableRequests.forEach(createTableRequest ->
                    new TestAmazonDynamoDbAdminUtils(testArgument.getAmazonDynamoDb())
                        .createTableIfNotExists(createTableRequest, getPollInterval()));
            });
            testArguments.add(testArgument);
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

        @Override
        public List<CreateTableRequest> getCreateTableRequests() {
            return createTableRequests;
            // Doesn't matter what the hashKeyAttrType is because the client only cares
            // which table has only a hk vs both hk and rk.  TODO msgroi make this return map of table names
            // keyed on whether they have hk only
            //            return getCreateRequests(S);
        }

        @Override
        public void teardown() {
            testArguments.forEach(testArgument -> testArgument.getOrgs().forEach(org -> {
                mtContext.setContext(org);
                getCreateRequests(testArgument.getHashKeyAttrType()).forEach(createTableRequest ->
                    new TestAmazonDynamoDbAdminUtils(testArgument.getAmazonDynamoDb())
                        .deleteTableIfExists(createTableRequest.getTableName(), getPollInterval(), TIMEOUT_SECONDS));
            }));
        }
    }

    public interface DataSetup extends Consumer<TestArgument> {
        default void teardown() {}
    }

    class DefaultDataSetup implements DataSetup {

        private final TableSetup tableSetup;

        DefaultDataSetup(TableSetup tableSetup) {
            this.tableSetup = tableSetup;
        }

        @Override
        @SuppressWarnings("checkstyle:Indentation")
        public void accept(TestArgument testArgument) {
            Map<Boolean, List<CreateTableRequest>> tablesPartitionedByHasRangeKey =
                ImmutableList.of(TABLE1, TABLE2, TABLE3).stream().map(
                    this::getCreateTableRequest).collect(Collectors.partitioningBy(
                    createTableRequest -> createTableRequest.getKeySchema().stream().anyMatch(
                        keySchemaElement -> KeyType.valueOf(keySchemaElement.getKeyType()) == RANGE)));
            testArgument.getOrgs().forEach(org -> {
                mtContext.setContext(org);
                // hk-only tables
                tablesPartitionedByHasRangeKey.get(false)
                    .stream().map(CreateTableRequest::getTableName)
                        .forEach(table -> testArgument.getAmazonDynamoDb().putItem(
                            new PutItemRequest().withTableName(table)
                                .withItem(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                                    SOME_FIELD_VALUE + table + org))));
                // hk-rk tables
                tablesPartitionedByHasRangeKey.get(true)
                    .stream().map(CreateTableRequest::getTableName)
                    .forEach(table -> {
                        testArgument.getAmazonDynamoDb().putItem(
                            new PutItemRequest().withTableName(table)
                                .withItem(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                                    SOME_FIELD_VALUE + table + org)));
                        testArgument.getAmazonDynamoDb().putItem(
                            new PutItemRequest().withTableName(table)
                                .withItem(buildItemWithValues(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE,
                                    Optional.of(RANGE_KEY_VALUE + "2"),
                                    SOME_FIELD_VALUE + table + org + "2",
                                    Optional.of(INDEX_FIELD_VALUE))));
                    });
            });
        }

        private CreateTableRequest getCreateTableRequest(String table) {
            return tableSetup.getCreateTableRequests().stream().filter(
                createTableRequest -> createTableRequest.getTableName().equals(table)).findAny().get();
        }

    }

    private int getPollInterval() {
        return IS_LOCAL_DYNAMO ? 0 : 1;
    }

}