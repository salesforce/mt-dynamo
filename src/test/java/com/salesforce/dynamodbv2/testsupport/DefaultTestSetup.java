package com.salesforce.dynamodbv2.testsupport;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.GSI2_HK_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.GSI2_RK_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.GSI_HK_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.INDEX_FIELD;
import static com.salesforce.dynamodbv2.testsupport.ItemBuilder.RANGE_KEY_FIELD;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.GSI2_HK_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.GSI2_RK_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.GSI_HK_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_OTHER_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.IS_LOCAL_DYNAMO;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_N_MAX;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_N_MIN;
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
import com.google.common.collect.Iterables;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Performs default table creation and population logic.  To override table creation of data population logic,
 * provide your own TestSetup implementation.  To add more tables to the default set, extend this class, call
 * super.setupTest() from within your accept method.  See {@link DefaultArgumentProvider} for more details.
 *
 * @author msgroi
 */
public class DefaultTestSetup implements TestSetup {

    protected static final MtAmazonDynamoDbContextProvider mtContext = MT_CONTEXT;

    public static final String TABLE1 = "Table1"; // has hk
    public static final String TABLE2 = "Table2"; // has hk
    public static final String TABLE3 = "Table3"; // has hk, rk(S), GSI and LSI
    public static final String TABLE4 = "Table4"; // has hk, rk(N), GSI and LSI
    public static final String TABLE5 = "Table5"; // has hk, rk(S), GSI whose HK field is the table's RK field, no LSI
    public static final String[] ALL_TABLES = { TABLE1, TABLE2, TABLE3, TABLE4, TABLE5 };
    public static final String[] NO_TABLES = {};

    private final Set<String> tableNames;

    public DefaultTestSetup(String[] tableNames) {
        this.tableNames = Arrays.stream(tableNames).collect(Collectors.toSet());
    }

    @Override
    public void setupTest(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            mtContext.setContext(org);
            List<CreateTableRequest> createTableRequests =
                getCreateRequests(testArgument.getHashKeyAttrType(), tableNames);
            createTableRequests.forEach(createTableRequest -> {
                createTable(testArgument, createTableRequest);
                setupTableData(testArgument.getAmazonDynamoDb(),
                    testArgument.getHashKeyAttrType(),
                    org,
                    createTableRequest);
            });
        });
    }

    private void createTable(TestArgument testArgument, CreateTableRequest createTableRequest) {
        if (testArgument.getAmazonDynamoDbStrategy().useMultitenantTables()) {
            MT_CONTEXT.withContext(null,
                () -> testArgument.getAmazonDynamoDb().createMultitenantTable(createTableRequest));
        } else {
            new TestAmazonDynamoDbAdminUtils(testArgument.getAmazonDynamoDb())
                .createTableIfNotExists(createTableRequest, getPollInterval());
        }
    }

    protected void setupTableData(AmazonDynamoDB amazonDynamoDb,
                                  ScalarAttributeType hashKeyAttrType,
                                  String org,
                                  CreateTableRequest createTableRequest) {
        String table = createTableRequest.getTableName();
        final Optional<ScalarAttributeType> rangeKeyAttributeTypeOpt = getScalarAttributeType(createTableRequest,
            KeyType.RANGE);
        if (rangeKeyAttributeTypeOpt.isEmpty()) { // no range key
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
                            .withItem(getSimpleItemWithStringRk(hashKeyAttrType, table, org).build()));
                    amazonDynamoDb.putItem(
                        new PutItemRequest().withTableName(table)
                            .withItem(getAllIndexFieldsItemWithStringRk(hashKeyAttrType, table, org).build()));
                    break;
                case N:
                    IntStream.rangeClosed(RANGE_KEY_N_MIN, RANGE_KEY_N_MAX).forEach(i -> amazonDynamoDb
                        .putItem(new PutItemRequest().withTableName(table)
                            .withItem(ItemBuilder.builder(hashKeyAttrType, HASH_KEY_VALUE)
                                .someField(S, SOME_OTHER_FIELD_VALUE + table + org)
                                .rangeKey(N, String.valueOf(i))
                                .build())));
                    break;
                default:
                    throw new IllegalArgumentException("unsupported type " + rangeKeyAttributeTypeOpt.get()
                        + " encountered");
            }
        }
    }

    public static ItemBuilder getSimpleItemWithStringRk(ScalarAttributeType hashKeyAttrType, String table, String org) {
        return ItemBuilder.builder(hashKeyAttrType, HASH_KEY_VALUE)
            .someField(S, SOME_FIELD_VALUE + table + org)
            .rangeKey(S, TestSupport.RANGE_KEY_S_VALUE);
    }

    public static ItemBuilder getAllIndexFieldsItemWithStringRk(ScalarAttributeType hashKeyAttrType, String table,
                                                                String org) {
        return ItemBuilder.builder(hashKeyAttrType, HASH_KEY_VALUE)
            .someField(S, SOME_OTHER_FIELD_VALUE + table + org)
            .rangeKey(S, RANGE_KEY_OTHER_S_VALUE)
            .indexField(S, INDEX_FIELD_VALUE)
            .gsiHkField(S, GSI_HK_FIELD_VALUE)
            .gsi2HkField(S, GSI2_HK_FIELD_VALUE)
            .gsi2RkField(N, GSI2_RK_FIELD_VALUE);
    }

    protected MtAmazonDynamoDbContextProvider getMtContext() {
        return mtContext;
    }

    /**
     * Gets the {@code ScalarAttributeType} associated with the one key in the key schema with {@code KeyType}
     * {@code keyType}.
     */
    private Optional<ScalarAttributeType> getScalarAttributeType(CreateTableRequest createTableRequest,
                                                                 KeyType keyType) {
        final Optional<ScalarAttributeType> scalarAttributeTypeOpt;
        final Set<String> setWithKeyAttributeNameIfItExists = createTableRequest.getKeySchema().stream()
            .filter(keySchemaElement -> keyType.equals(KeyType.fromValue(keySchemaElement.getKeyType())))
            .map(KeySchemaElement::getAttributeName)
            .collect(Collectors.toSet());
        if (setWithKeyAttributeNameIfItExists.isEmpty()) {
            scalarAttributeTypeOpt = Optional.empty();
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
            scalarAttributeTypeOpt = Optional.of(Iterables.getOnlyElement(setWithKeyAttributeType));
        }

        return scalarAttributeTypeOpt;
    }

    private List<CreateTableRequest> getCreateRequests(ScalarAttributeType hashKeyAttrType, Set<String> tableNames) {
        List<CreateTableRequest> tables = new ArrayList<>(tableNames.size());

        /*
         * Each successive table adds onto the previous table builder state that was used to create the previous table.
         */
        // TABLE1
        CreateTableRequestBuilder baseBuilder = CreateTableRequestBuilder.builder()
            .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, hashKeyAttrType))
            .withKeySchema(getKeySchema(HASH_KEY_FIELD, HASH))
            .withProvisionedThroughput(1L, 1L);
        if (tableNames.contains(TABLE1)) {
            tables.add(baseBuilder.withTableName(TABLE1).build());
        }
        // TABLE2
        if (tableNames.contains(TABLE2)) {
            tables.add(baseBuilder.withTableName(TABLE2).build());
        }
        // TABLE3: has an RK, GSIs, and LSIs
        baseBuilder
            .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, hashKeyAttrType),
                new AttributeDefinition(RANGE_KEY_FIELD, S),
                new AttributeDefinition(INDEX_FIELD, S),
                new AttributeDefinition(GSI_HK_FIELD, S),
                new AttributeDefinition(GSI2_HK_FIELD, S),
                new AttributeDefinition(GSI2_RK_FIELD, N))
            .withKeySchema(getKeySchema(HASH_KEY_FIELD, HASH, RANGE_KEY_FIELD, RANGE))
            .withGlobalSecondaryIndexes(
                buildGsi("testGsi", getKeySchema(GSI_HK_FIELD, HASH)),
                buildGsi("testGsi2", getKeySchema(GSI2_HK_FIELD, HASH, GSI2_RK_FIELD, RANGE)))
            .withLocalSecondaryIndexes(
                buildLsi("testLsi", getKeySchema(HASH_KEY_FIELD, HASH, INDEX_FIELD, RANGE)));
        if (tableNames.contains(TABLE3)) {
            tables.add(baseBuilder.withTableName(TABLE3).build());
        }
        // TABLE4: same as TABLE3, but with different with RK of type N
        baseBuilder
            .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, hashKeyAttrType),
                new AttributeDefinition(RANGE_KEY_FIELD, N), // unlike TABLE3
                new AttributeDefinition(INDEX_FIELD, S),
                new AttributeDefinition(GSI_HK_FIELD, S),
                new AttributeDefinition(GSI2_HK_FIELD, S),
                new AttributeDefinition(GSI2_RK_FIELD, N));
        if (tableNames.contains(TABLE4)) {
            tables.add(baseBuilder.withTableName(TABLE4).build());
        }
        // TABLE5: has a GSI whose HK field is the table's RK field, two GSIs with a common field, and no LSI
        baseBuilder
            .withAttributeDefinitions(
                new AttributeDefinition(HASH_KEY_FIELD, hashKeyAttrType),
                new AttributeDefinition(RANGE_KEY_FIELD, S),
                new AttributeDefinition(GSI_HK_FIELD, S),
                new AttributeDefinition(INDEX_FIELD, S),
                new AttributeDefinition(GSI2_RK_FIELD, N))
            .withKeySchema(getKeySchema(HASH_KEY_FIELD, HASH, RANGE_KEY_FIELD, RANGE))
            .withGlobalSecondaryIndexes(
                buildGsi("testGsi_table_rk_as_index_hk", getKeySchema(RANGE_KEY_FIELD, HASH)),
                buildGsi("testGsi_on_common_field_1", getKeySchema(GSI_HK_FIELD, HASH, INDEX_FIELD, RANGE)),
                buildGsi("testGsi_on_common_field_2", getKeySchema(GSI_HK_FIELD, HASH, GSI2_RK_FIELD, RANGE)))
            .withLocalSecondaryIndexes();
        if (tableNames.contains(TABLE5)) {
            tables.add(baseBuilder.withTableName(TABLE5).build());
        }

        return tables;
    }

    private KeySchemaElement[] getKeySchema(String hkName, KeyType hkType) {
        return new KeySchemaElement[] { new KeySchemaElement(hkName, hkType) };
    }

    private KeySchemaElement[] getKeySchema(String hkName, KeyType hkType, String rkName, KeyType rkType) {
        return new KeySchemaElement[] {
            new KeySchemaElement(hkName, hkType),
            new KeySchemaElement(rkName, rkType)
        };
    }

    private GlobalSecondaryIndex buildGsi(String name, KeySchemaElement... keySchemaElements) {
        return new GlobalSecondaryIndex()
            .withIndexName(name)
            .withKeySchema(keySchemaElements)
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withProjection(new Projection().withProjectionType(ProjectionType.ALL));
    }

    private LocalSecondaryIndex buildLsi(String name, KeySchemaElement... keySchemaElements) {
        return new LocalSecondaryIndex()
            .withIndexName(name)
            .withKeySchema(keySchemaElements)
            .withProjection(new Projection().withProjectionType(ProjectionType.ALL));
    }

    protected int getPollInterval() {
        return IS_LOCAL_DYNAMO ? 0 : 1;
    }

}