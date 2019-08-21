package com.salesforce.dynamodbv2.testsupport;

import static com.salesforce.dynamodbv2.testsupport.TestSupport.IS_LOCAL_DYNAMO;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.dynamodblocal.LocalDynamoDbServer;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount.MtAccountMapper;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByTable;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbLogger;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    private static final AmazonDynamoDB ROOT_AMAZON_DYNAMO_DB = IS_LOCAL_DYNAMO
        ? AmazonDynamoDbLocal.getAmazonDynamoDbLocal()
        : AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
    private static final AtomicInteger ORG_COUNTER = new AtomicInteger();
    public static final int ORGS_PER_TEST = 2;
    private static final boolean LOGGING_ENABLED = false; // log DDL and DML operations
    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
        new MtAmazonDynamoDbContextProviderThreadLocalImpl();

    private AmazonDynamoDB rootAmazonDynamoDb = ROOT_AMAZON_DYNAMO_DB;

    public ArgumentBuilder() {
    }

    public ArgumentBuilder(AmazonDynamoDB rootAmazonDynamoDb) {
        this.rootAmazonDynamoDb = rootAmazonDynamoDb;
    }

    @Override
    public List<TestArgument> get() {
        List<TestArgument> ret = Lists.newArrayList();
        for (AmazonDynamoDB mtStrategy : getAmazonDynamoDbStrategies()) {
            for (ScalarAttributeType hashKeyAttributes : getHashKeyAttrTypes()) {
                ret.add(new TestArgument(mtStrategy, getOrgs(), hashKeyAttributes, rootAmazonDynamoDb));
            }
        }
        return ret;
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
        // TODO get-bug: remove the filter and fix the tests that fail when RK is Binary
        return Arrays.stream(ScalarAttributeType.values()).filter(scalarAttributeType ->
            !scalarAttributeType.equals(ScalarAttributeType.B)).collect(Collectors.toList());
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
         * bySharedTable
         */
        AmazonDynamoDB sharedTable = SharedTableBuilder.builder()
            .withPollIntervalSeconds(getPollInterval())
            .withAmazonDynamoDb(amazonDynamoDb)
            .withContext(MT_CONTEXT)
            .withTruncateOnDeleteTable(true).build();

        /*
         * bySharedTable w/ binary hash key
         */
        AmazonDynamoDB sharedTableBinaryHashKey = SharedTableBuilder.builder()
            .withPollIntervalSeconds(getPollInterval())
            .withAmazonDynamoDb(amazonDynamoDb)
            .withContext(MT_CONTEXT)
            .withTruncateOnDeleteTable(true)
            .withBinaryHashKey(true)
            .build();

        return ImmutableList.of(
            /*
             * Testing byAccount by itself and with byTable succeeds, but SQLite failures occur when it runs
             * concurrently with any of the sharedTable* strategies.
             */
            //byAccount,
            byTable,
            sharedTable,
            sharedTableBinaryHashKey
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

        final Map<String, LocalDynamoDbServer> contextServerMap = new HashMap<>();

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
        private final AmazonDynamoDB amazonDynamoDb;
        private final AmazonDynamoDB rootAmazonDynamoDb;
        private final List<String> orgs;
        private final ScalarAttributeType hashKeyAttrType;

        /**
         * Takes the arguments that make up the inputs to a test invocation.
         */
        TestArgument(AmazonDynamoDB amazonDynamoDb, List<String> orgs,
                     ScalarAttributeType hashKeyAttrType, AmazonDynamoDB rootAmazonDynamoDb) {
            this.amazonDynamoDb = amazonDynamoDb;
            this.orgs = orgs;
            this.hashKeyAttrType = hashKeyAttrType;
            this.rootAmazonDynamoDb = rootAmazonDynamoDb;
        }

        public AmazonDynamoDB getAmazonDynamoDb() {
            return amazonDynamoDb;
        }

        public AmazonDynamoDB getRootAmazonDynamoDb() {
            return rootAmazonDynamoDb;
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