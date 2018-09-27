package com.salesforce.dynamodbv2.testsupport;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.IS_LOCAL_DYNAMO;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.dynamodblocal.LocalDynamoDbServer;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount.MtAccountMapper;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByTable;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbLogger;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.CreateTableRequestFactory;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableCustomDynamicBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableCustomStaticBuilder;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Builds of a list of {@code TestArgument}s.  Each {@code TestArgument} consists of 3 elements:
 *
 * <p>- the {@code AmazonDynamoDB} instance to be tested
 * - the attribute type of the hash key of the table to be tested
 * - a list of orgs that have been designated to be used for the given test invocation
 *
 * <p>The {@code ArgumentBuilder} is used by the {@code DefaultArgumentProvider} which can be referenced in a JUnit 5
 * {@code @ParameterizedTest} {@code @ArgumentSource} annotation.  See {@link DefaultArgumentProvider} for details.
 *
 * @author msgroi
 */
public class ArgumentBuilder implements Supplier<List<TestArgument>> {

    static final Regions REGION = Regions.US_EAST_1;
    @VisibleForTesting
    public static final AmazonDynamoDB ROOT_AMAZON_DYNAMO_DB = IS_LOCAL_DYNAMO
        ? AmazonDynamoDbLocal.getAmazonDynamoDbLocal()
        : AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
    private static final AtomicInteger ORG_COUNTER = new AtomicInteger();
    public static final int ORGS_PER_TEST = 2;
    private static final boolean LOGGING_ENABLED = false; // log DDL and DML operations
    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT = new MtAmazonDynamoDbContextProviderImpl();

    private AmazonDynamoDB rootAmazonDynamoDb = ROOT_AMAZON_DYNAMO_DB;
    private static final String HK_TABLE_NAME = "hkTable";
    private static final String HK_RK_S_TABLE_NAME = "hkRkSTable"; // range-key type: S
    private static final String HK_RK_N_TABLE_NAME = "hkRkNTable"; // range-key type: N
    private static final String HASH_KEY_FIELD = "HASH_KEY_FIELD";
    private static final String RANGE_KEY_FIELD = "RANGE_KEY_FIELD";
    private static final String INDEX_FIELD = "INDEX_FIELD";
    private static final String INDEX_RANGE_FIELD = "INDEX_RANGE_FIELD";

    public ArgumentBuilder() {
    }

    public ArgumentBuilder(AmazonDynamoDB rootAmazonDynamoDb) {
        this.rootAmazonDynamoDb = rootAmazonDynamoDb;
    }

    @Override
    public List<TestArgument> get() {
        return getAmazonDynamoDbStrategies().stream().flatMap(
            (Function<AmazonDynamoDB, Stream<TestArgument>>) amazonDynamoDB ->
                getHashKeyAttrTypes().stream().flatMap(
                    (Function<ScalarAttributeType, Stream<TestArgument>>) scalarAttributeType ->
                        Stream.of(new TestArgument(amazonDynamoDB,
                            getOrgs(),
                            scalarAttributeType)))).collect(Collectors.toList());
    }

    /*
     * Returns a list of orgs to be used for a test.
     */
    private List<String> getOrgs() {
        return IntStream.rangeClosed(1, ORGS_PER_TEST).mapToObj(i -> "Org-" + ORG_COUNTER.incrementAndGet()).collect(
            Collectors.toList());
    }

    /*
     * Returns a list of DynamoDB data types to be used as the table's HASH key data type when creating virtual tables.
     */
    private List<ScalarAttributeType> getHashKeyAttrTypes() {
        return Arrays.asList(ScalarAttributeType.values());
    }

    /*
     * Returns a list of AmazonDynamoDB instances to be tested.
     */
    private List<AmazonDynamoDB> getAmazonDynamoDbStrategies() {
        AmazonDynamoDB amazonDynamoDb = wrapWithLogger(rootAmazonDynamoDb);

        /*
         * byAccount
         */
        AmazonDynamoDB byAccount = MtAmazonDynamoDbByAccount.accountMapperBuilder()
            .withAccountMapper(new DynamicAccountMtMapper())
            .withContext(MT_CONTEXT).build();

        /*
         * byTable
         */
        AmazonDynamoDB byTable = MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(amazonDynamoDb)
            .withContext(MT_CONTEXT).build();

        /*
         * SharedTableCustomDynamicBuilder
         */
        String hashKeyField = "hashKeyField";
        String rangeKeyField = "rangeKeyField";
        String indexField = "indexField";
        String indexRangeField = "indexRangeField";
        CreateTableRequestFactory createTableRequestFactory = new CreateTableRequestFactory() {
            @Override
            public Optional<CreateTableRequest> getCreateTableRequest(DynamoTableDescription virtualTableDescription) {
                final List<CreateTableRequest> physicalTables = getPhysicalTables();
                final String virtualTableName = virtualTableDescription.getTableName();
                final CreateTableRequest retvalInnards;
                final char virtualTableNameLastChar = virtualTableName.charAt(virtualTableName.length() - 1);
                boolean isDefault = false;
                switch (virtualTableNameLastChar) {
                    case '3':
                        retvalInnards = physicalTables.get(0);
                        break;
                    case '4':
                        retvalInnards = physicalTables.get(1);
                        break;
                    default:
                        isDefault = true;
                        retvalInnards = physicalTables.get(2);
                }

                final String physicalTableName = retvalInnards.getTableName();
                Preconditions.checkState(isDefault || physicalTableName.charAt(physicalTableName.length() - 1)
                    == virtualTableNameLastChar);
                return Optional.of(retvalInnards);
            }

            @Override
            public List<CreateTableRequest> getPhysicalTables() {
                return ImmutableList.of(
                        new CreateTableRequest()
                                .withTableName("table_3")
                                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                                .withAttributeDefinitions(new AttributeDefinition(hashKeyField, S),
                                        new AttributeDefinition(rangeKeyField, S),
                                        new AttributeDefinition(indexField, S),
                                        new AttributeDefinition(indexRangeField, S))
                                .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH),
                                        new KeySchemaElement(rangeKeyField, KeyType.RANGE))
                                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                                        .withKeySchema(new KeySchemaElement(indexField, KeyType.HASH))
                                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                                .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                                        .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH),
                                                new KeySchemaElement(indexRangeField, KeyType.RANGE))
                                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                                .withStreamSpecification(new StreamSpecification()
                                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                                        .withStreamEnabled(true)),
                        new CreateTableRequest()
                                .withTableName("table_4")
                                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                                .withAttributeDefinitions(new AttributeDefinition(hashKeyField, S),
                                        new AttributeDefinition(rangeKeyField, N),
                                        new AttributeDefinition(indexField, S),
                                        new AttributeDefinition(indexRangeField, S))
                                .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH),
                                        new KeySchemaElement(rangeKeyField, KeyType.RANGE))
                                .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                                        .withKeySchema(new KeySchemaElement(indexField, KeyType.HASH))
                                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                                .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                                        .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH),
                                                new KeySchemaElement(indexRangeField, KeyType.RANGE))
                                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                                .withStreamSpecification(new StreamSpecification()
                                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                                        .withStreamEnabled(true)),
                        new CreateTableRequest()
                                .withTableName("table_default")
                                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                                .withAttributeDefinitions(new AttributeDefinition(hashKeyField, S))
                                .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH))
                                .withStreamSpecification(new StreamSpecification()
                                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                                        .withStreamEnabled(true))
                );
            }
        };

        AmazonDynamoDB sharedTableCustomDynamic = SharedTableCustomDynamicBuilder.builder()
            .withPollIntervalSeconds(getPollInterval())
            .withAmazonDynamoDb(amazonDynamoDb)
            .withContext(MT_CONTEXT)
            .withCreateTableRequestFactory(createTableRequestFactory)
            .withTruncateOnDeleteTable(true).build();

        /*
         * sharedTableCustomStaticBuilder
         */
        AmazonDynamoDB sharedTableCustomStaticBuilder = SharedTableCustomStaticBuilder.builder()
            .withCreateTableRequests(
                new CreateTableRequest()
                    .withTableName(HK_TABLE_NAME)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S))
                    .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true)),
                new CreateTableRequest()
                    .withTableName(HK_RK_S_TABLE_NAME)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S),
                        new AttributeDefinition(RANGE_KEY_FIELD, S),
                        new AttributeDefinition(INDEX_FIELD, S),
                        new AttributeDefinition(INDEX_RANGE_FIELD, S))
                    .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                        new KeySchemaElement(RANGE_KEY_FIELD, KeyType.RANGE))
                    .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                        .withKeySchema(new KeySchemaElement(INDEX_FIELD, KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                        .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                            new KeySchemaElement(INDEX_RANGE_FIELD, KeyType.RANGE))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true)),
                new CreateTableRequest()
                    .withTableName(HK_RK_N_TABLE_NAME)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withAttributeDefinitions(new AttributeDefinition(HASH_KEY_FIELD, S),
                        new AttributeDefinition(RANGE_KEY_FIELD, N),
                        new AttributeDefinition(INDEX_FIELD, S),
                        new AttributeDefinition(INDEX_RANGE_FIELD, S))
                    .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                        new KeySchemaElement(RANGE_KEY_FIELD, KeyType.RANGE))
                    .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("testgsi")
                        .withKeySchema(new KeySchemaElement(INDEX_FIELD, KeyType.HASH))
                        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("testlsi")
                        .withKeySchema(new KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH),
                            new KeySchemaElement(INDEX_RANGE_FIELD, KeyType.RANGE))
                        .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true)))
            .withTableMapper(virtualTableDescription -> {
                final String virtualTableName = virtualTableDescription.getTableName();
                switch (virtualTableName.charAt(virtualTableName.length() - 1)) {
                    case '3':
                        return HK_RK_S_TABLE_NAME;
                    case '4':
                        return HK_RK_N_TABLE_NAME;
                    default:
                        return HK_TABLE_NAME;
                }
            })
            .withAmazonDynamoDb(amazonDynamoDb)
            .withContext(MT_CONTEXT)
            .withTruncateOnDeleteTable(true)
            .withPollIntervalSeconds(getPollInterval()).build();

        /*
         * bySharedTable
         */
        AmazonDynamoDB sharedTable = SharedTableBuilder.builder()
            .withPollIntervalSeconds(getPollInterval())
            .withAmazonDynamoDb(amazonDynamoDb)
            .withContext(MT_CONTEXT)
            .withTruncateOnDeleteTable(true).build();

        return ImmutableList.of(
            /*
             * Testing byAccount by itself and with byTable succeeds, but sqlite failures occur when it runs
             * concurrently with any of the sharedTable* strategies.
             */
            //byAccount,
            byTable,
            sharedTableCustomDynamic,
            sharedTableCustomStaticBuilder,
            sharedTable
        );
    }

    private static int getPollInterval() {
        return (IS_LOCAL_DYNAMO ? 0 : 5);
    }

    private static AmazonDynamoDB wrapWithLogger(AmazonDynamoDB amazonDynamoDb) {
        return LOGGING_ENABLED ? MtAmazonDynamoDbLogger.builder()
            .withAmazonDynamoDb(amazonDynamoDb)
            .withContext(MT_CONTEXT)
            .withMethodsToLog(
                ImmutableList.of("batchGetItem", "createTable", "deleteItem", "deleteTable", "describeTable", "getItem",
                    "putItem", "query", "scan", "updateItem")).build() : amazonDynamoDb;
    }

    private static class DynamicAccountMtMapper implements MtAccountMapper {

        Map<String, LocalDynamoDbServer> contextServerMap = new HashMap<>();

        @Override
        public AmazonDynamoDB getAmazonDynamoDb(MtAmazonDynamoDbContextProvider mtContext) {
            return contextServerMap.computeIfAbsent(mtContext.getContext(), org -> {
                LocalDynamoDbServer server = new LocalDynamoDbServer();
                server.start();
                return server;
            }).getClient();
        }

        @Override
        public void shutdown() {
            contextServerMap.values().forEach(LocalDynamoDbServer::stop);
        }

    }

    /**
     * Represents an AmazonDynamoDB to be tested along with a list of orgs that have been designated to be used
     * when testing that instance.  See the {@link ArgumentBuilder} Javadoc for details.
     */
    public static class TestArgument {
        private AmazonDynamoDB amazonDynamoDb;
        private List<String> orgs;
        private ScalarAttributeType hashKeyAttrType;

        /**
         * Takes the arguments that make up the inputs to a test invocation.
         */
        public TestArgument(AmazonDynamoDB amazonDynamoDb, List<String> orgs,
            ScalarAttributeType hashKeyAttrType) {
            this.amazonDynamoDb = amazonDynamoDb;
            this.orgs = orgs;
            this.hashKeyAttrType = hashKeyAttrType;
        }

        public AmazonDynamoDB getAmazonDynamoDb() {
            return amazonDynamoDb;
        }

        public List<String> getOrgs() {
            return orgs;
        }

        public ScalarAttributeType getHashKeyAttrType() {
            return hashKeyAttrType;
        }

        public void forEachOrgContext(Consumer<String> consumer) {
            orgs.forEach(org -> MT_CONTEXT.withContext(org, () -> consumer.accept(org)));
        }

        @Override
        public String toString() {
            return amazonDynamoDb.getClass().getSimpleName()
                + ", orgs=" + orgs
                + ", hashKeyAttrType=" + hashKeyAttrType.name();
        }
    }

}