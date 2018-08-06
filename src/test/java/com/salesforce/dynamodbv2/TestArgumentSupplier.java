package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.TestSupport.IS_LOCAL_DYNAMO;

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
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.google.common.collect.ImmutableList;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
class TestArgumentSupplier implements Supplier<List<Arguments>> {

    static final Regions REGION = Regions.US_EAST_1;
    private static final AmazonDynamoDB ROOT_AMAZON_DYNAMO_DB = IS_LOCAL_DYNAMO
        ? AmazonDynamoDbLocal.getAmazonDynamoDbLocal()
        : AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
    private static final AtomicInteger ORG_COUNTER = new AtomicInteger();
    private static final int ORGS_PER_TEST = 3; // TODO msgroi 2?
    private static final boolean LOGGING_ENABLED = false; // log DDL and DML operations
    private static final int DYNAMO_BASE_PORT = 8001;
    private static List<LocalDynamoDBServer> servers;
    static final MtAmazonDynamoDbContextProvider MT_CONTEXT = new MtAmazonDynamoDbContextProviderImpl();

    private AmazonDynamoDB rootAmazonDynamoDb = ROOT_AMAZON_DYNAMO_DB;

    public TestArgumentSupplier() {
    }

    public TestArgumentSupplier(AmazonDynamoDB rootAmazonDynamoDb) {
        this.rootAmazonDynamoDb = rootAmazonDynamoDb;
    }

    @Override
    public List<Arguments> get() {
        return getAmazonDynamoDBStrategies().stream()
            .map(amazonDynamoDB -> Arguments.of(new TestArgument(
                amazonDynamoDB,
                getOrgs().collect(Collectors.toList())))).collect(Collectors.toList());
    }

    /*
     * Returns a list of orgs to be used for a test.
     */
    private Stream<String> getOrgs() {
        return IntStream.rangeClosed(1, ORGS_PER_TEST).mapToObj(i -> "Org-" + ORG_COUNTER.incrementAndGet());
    }

    /*
     * Returns a list of AmazonDynamoDB instances to be tested.
     */
    private List<AmazonDynamoDB> getAmazonDynamoDBStrategies() {
        AmazonDynamoDB amazonDynamoDB = wrapWithLogger(rootAmazonDynamoDb);

        /*
         * byAccount
         */
        AmazonDynamoDB byAccount = MtAmazonDynamoDbByAccount.accountMapperBuilder()
            .withAccountMapper(new MtAccountMapper() {
                @Override
                public AmazonDynamoDB getAmazonDynamoDb(MtAmazonDynamoDbContextProvider mtContext) {
                    return wrapWithLogger(getServers().get(Integer.valueOf(mtContext.getContext().split("-")[1]) % ORGS_PER_TEST).start());
                }
                @Override
                public void shutdown() {
                    getServers().forEach(LocalDynamoDBServer::stop);
                }
            })
            .withContext(MT_CONTEXT).build();

        /*
         * byTable
         */
        AmazonDynamoDB byTable = MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(amazonDynamoDB)
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
            .withAmazonDynamoDb(amazonDynamoDB)
            .withContext(MT_CONTEXT)
            .withCreateTableRequestFactory(createTableRequestFactory)
            .withTruncateOnDeleteTable(true).build();

        /*
         * sharedTableCustomStaticBuilder
         */
        String HK_TABLE_NAME = "hkTable";
        String HK_RK_TABLE_NAME = "hkRkTable";
        String HASH_KEY_FIELD = "HASH_KEY_FIELD";
        String RANGE_KEY_FIELD = "RANGE_KEY_FIELD";
        String INDEX_FIELD = "INDEX_FIELD";
        String INDEX_RANGE_FIELD = "INDEX_RANGE_FIELD";

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
            .withAmazonDynamoDb(amazonDynamoDB)
            .withContext(MT_CONTEXT)
            .withTruncateOnDeleteTable(true)
            .withPollIntervalSeconds(getPollInterval()).build();

        /*
         * bySharedTable
         */
        AmazonDynamoDB sharedTable = SharedTableBuilder.builder()
            .withPollIntervalSeconds(getPollInterval())
            .withAmazonDynamoDb(amazonDynamoDB)
            .withContext(MT_CONTEXT)
            .withTruncateOnDeleteTable(true).build();

        return ImmutableList.of(
            /*
             * The byAccount works by itself and with byTable, but sqlite failures occur when it runs concurrently with any of
             * the sharedTable* strategies.
             */
//            byAccount,
            byTable,
            sharedTableCustomDynamic,
            sharedTableCustomStaticBuilder,
            sharedTable
        );
    }

    private static int getPollInterval() {
        return (IS_LOCAL_DYNAMO ? 0 : 5);
    }

    private static AmazonDynamoDB wrapWithLogger(AmazonDynamoDB amazonDynamoDB) {
        return LOGGING_ENABLED ? MtAmazonDynamoDbLogger.builder()
            .withAmazonDynamoDb(amazonDynamoDB)
            .withContext(MT_CONTEXT)
            .withMethodsToLog(
                ImmutableList.of("createTable", "deleteItem", "deleteTable", "describeTable", "getItem",
                    "putItem", "query", "scan", "updateItem")).build() : amazonDynamoDB;
    }

    private static List<LocalDynamoDBServer> getServers() {
        if (servers == null) {
            servers = IntStream.rangeClosed(DYNAMO_BASE_PORT, DYNAMO_BASE_PORT + ORGS_PER_TEST)
                .mapToObj(LocalDynamoDBServer::new).collect(Collectors.toList());
        }
        return servers;
    }

    /*
     * Represents an AmazonDynamoDB to be tested along with a list of orgs that have been designated to be used
     * when testing that instance.
     */
    static class TestArgument {
        private AmazonDynamoDB amazonDynamoDB;
        List<String> orgs;

        public TestArgument(AmazonDynamoDB amazonDynamoDB,
            List<String> orgs) {
            this.amazonDynamoDB = amazonDynamoDB;
            this.orgs = orgs;
        }

        AmazonDynamoDB getAmazonDynamoDB() {
            return amazonDynamoDB;
        }

        List<String> getOrgs() {
            return orgs;
        }

        @Override
        public String toString() {
            return amazonDynamoDB.getClass().getSimpleName() + ", orgs=" + orgs + '}';
        }
    }

}