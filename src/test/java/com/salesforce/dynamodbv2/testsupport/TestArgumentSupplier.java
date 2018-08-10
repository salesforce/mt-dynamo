package com.salesforce.dynamodbv2.testsupport;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
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
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.dynamodblocal.LocalDynamoDbServer;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount.MtAccountMapper;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByTable;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbLogger;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.CreateTableRequestFactory;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableCustomDynamicBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableCustomStaticBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

/*
 * Supplies of a list of Argument objects, where each Argument is a 2 element Object[] array, where the first element
 * is the AmazonDynamoDB instance being tested and the second element is a list of orgs that have been designated
 * to be used when testing that instance.
 *
 * @author msgroi
 */
public class TestArgumentSupplier implements Supplier<List<Arguments>> {

    static final Regions REGION = Regions.US_EAST_1;
    private static final AmazonDynamoDB ROOT_AMAZON_DYNAMO_DB = IS_LOCAL_DYNAMO
        ? AmazonDynamoDbLocal.getAmazonDynamoDbLocal()
        : AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
    private static final AtomicInteger ORG_COUNTER = new AtomicInteger();
    private static final int ORGS_PER_TEST = 2;
    private static final boolean LOGGING_ENABLED = false; // log DDL and DML operations
    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT = new MtAmazonDynamoDbContextProviderImpl();

    private AmazonDynamoDB rootAmazonDynamoDb = ROOT_AMAZON_DYNAMO_DB;
    private static final String HK_TABLE_NAME = "hkTable";
    private static final String HK_RK_TABLE_NAME = "hkRkTable";
    private static final String HASH_KEY_FIELD = "HASH_KEY_FIELD";
    private static final String RANGE_KEY_FIELD = "RANGE_KEY_FIELD";
    private static final String INDEX_FIELD = "INDEX_FIELD";
    private static final String INDEX_RANGE_FIELD = "INDEX_RANGE_FIELD";

    public TestArgumentSupplier() {
    }

    public TestArgumentSupplier(AmazonDynamoDB rootAmazonDynamoDb) {
        this.rootAmazonDynamoDb = rootAmazonDynamoDb;
    }

    @Override
    public List<Arguments> get() {
        return getAmazonDynamoDbStrategies().stream().flatMap(
            (Function<AmazonDynamoDB, Stream<Arguments>>) amazonDynamoDB ->
                getHashKeyAttrTypes().stream().flatMap(
                    (Function<ScalarAttributeType, Stream<Arguments>>) scalarAttributeType ->
                        Stream.of(Arguments.of(new TestArgument(amazonDynamoDB,
                            getOrgs(),
                            scalarAttributeType))))).collect(Collectors.toList());
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
        return ImmutableList.of(S, N, B);
    }

    /*
     * Returns a list of AmazonDynamoDB instances to be tested.
     */
    private List<AmazonDynamoDB> getAmazonDynamoDbStrategies() {
        AmazonDynamoDB amazonDynamoDb = wrapWithLogger(rootAmazonDynamoDb);

        /*
         * byAccount
         */
        AmazonDynamoDB byAccount = MtAmazonDynamoDbByAccount.accountMapperBuilder() // TODO msgroi test byAccount again
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
        CreateTableRequestFactory createTableRequestFactory = virtualTableDescription -> {
            String tableName = virtualTableDescription.getTableName();
            if (tableName.endsWith("3")) {
                return new CreateTableRequest()
                    .withTableName(tableName)
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
                        .withStreamEnabled(true));
            } else {
                return new CreateTableRequest()
                    .withTableName(tableName)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                    .withAttributeDefinitions(new AttributeDefinition(hashKeyField, S))
                    .withKeySchema(new KeySchemaElement(hashKeyField, KeyType.HASH))
                    .withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true));
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
                    .withTableName(HK_RK_TABLE_NAME)
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
                        .withStreamEnabled(true)))
            .withTableMapper(virtualTableDescription ->
                virtualTableDescription.getTableName().endsWith("3") ? HK_RK_TABLE_NAME : HK_TABLE_NAME)
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
                ImmutableList.of("createTable", "deleteItem", "deleteTable", "describeTable", "getItem",
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

    /*
     * Represents an AmazonDynamoDB to be tested along with a list of orgs that have been designated to be used
     * when testing that instance.
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

        @Override
        public String toString() {
            return amazonDynamoDb.getClass().getSimpleName()
                + ", orgs=" + orgs + '}'
                + ", hashKeyAttrType=" + hashKeyAttrType.name();
        }
    }

}