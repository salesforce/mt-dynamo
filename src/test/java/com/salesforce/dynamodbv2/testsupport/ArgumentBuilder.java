package com.salesforce.dynamodbv2.testsupport;

import static com.salesforce.dynamodbv2.testsupport.TestSupport.IS_LOCAL_DYNAMO;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.dynamodblocal.LocalDynamoDbServer;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbBase;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount.MtAccountMapper;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByTable;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbComposite;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbLogger;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningStrategy;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Builds of a list of {@code TestArgument}s.  Each {@code TestArgument} consists of 3 elements:
 *
 * <ul><li> the {@code AmazonDynamoDB} instance to be tested
 * <li> the attribute type of the hash key of the table to be tested
 * <li> a list of orgs that have been designated to be used for the given test invocation</ul>
 *
 * <p>{@code ArgumentBuilder} is used by the {@code DefaultArgumentProvider}, which can be referenced in a JUnit 5
 * {@code @ParameterizedTest} {@code @ArgumentSource} annotation.  See {@link DefaultArgumentProvider} for details.
 *
 * @author msgroi
 */
public class ArgumentBuilder implements Supplier<List<TestArgument>> {

    static final Regions REGION = Regions.US_EAST_1;
    private static final AmazonDynamoDB DEFAULT_ROOT_AMAZON_DYNAMO_DB = IS_LOCAL_DYNAMO
        ? AmazonDynamoDbLocal.getAmazonDynamoDbLocal()
        : AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
    public static final MtAmazonDynamoDbContextProvider MT_CONTEXT =
        new MtAmazonDynamoDbContextProviderThreadLocalImpl();
    private static final AtomicInteger ORG_COUNTER = new AtomicInteger();
    public static final int ORGS_PER_TEST = 2;
    private static final boolean LOGGING_ENABLED = false; // log DDL and DML operations

    public enum AmazonDynamoDbStrategy {

        ByAccount(root -> MtAmazonDynamoDbByAccount.accountMapperBuilder()
            .withAccountMapper(new DynamicAccountMtMapper())
            .withContext(MT_CONTEXT).build()),

        ByTable(root -> MtAmazonDynamoDbByTable.builder()
            .withAmazonDynamoDb(wrapWithLogger(root))
            .withContext(MT_CONTEXT).build()),

        RandomPartitioning(root -> getSharedTableBuilder(root, "RandomPartitioning.").build()),

        RandomPartitioningBinaryHk(root -> getSharedTableBuilder(root, "RandomPartitioningBinaryHk.")
            .withBinaryHashKey(true)
            .build()),

        HashPartitioning(root -> getTrivialCompositeClient(
            addHashPartitioning(getSharedTableBuilder(root, "HashPartitioning.")).build())),

        // strategy where default test tables are created as multitenant tables
        HashPartitioningMt(root -> buildHashPartitioningMt(root, "HashPartitioningMt."), true);

        private static MtAmazonDynamoDbBase buildHashPartitioningMt(AmazonDynamoDB root, String prefix) {
            Set<String> testTables = Arrays.stream(DefaultTestSetup.ALL_TABLES).collect(Collectors.toSet());
            Predicate<String> isMultitenantVirtualTable = t -> testTables.contains(t)
                || t.startsWith(TestSupport.MT_VIRTUAL_TABLE_PREFIX);
            return addHashPartitioning(getSharedTableBuilder(root, prefix))
                .withMultitenantVirtualTableCheck(isMultitenantVirtualTable)
                .build();
        }

        private static SharedTableBuilder addHashPartitioning(SharedTableBuilder builder) {
            return builder
                .withBinaryHashKey(true)
                .withPartitioningStrategy(new HashPartitioningStrategy(64));
        }

        private static SharedTableBuilder getSharedTableBuilder(AmazonDynamoDB root, String prefix) {
            return SharedTableBuilder.builder()
                .withTablePrefix(prefix)
                .withPollIntervalSeconds(getPollInterval())
                .withAmazonDynamoDb(wrapWithLogger(root))
                .withContext(MT_CONTEXT)
                .withTruncateOnDeleteTable(true)
                .withMultitenantVirtualTableCheck(t -> t.startsWith(TestSupport.MT_VIRTUAL_TABLE_PREFIX));
        }

        private static MtAmazonDynamoDbComposite getTrivialCompositeClient(MtAmazonDynamoDbBase delegate) {
            return new MtAmazonDynamoDbComposite(Collections.singletonList(delegate),
                () -> delegate, table -> delegate);
        }

        public static final List<AmazonDynamoDbStrategy> SHARED_TABLE_STRATEGIES = ImmutableList.of(
            RandomPartitioning, RandomPartitioningBinaryHk, HashPartitioning);

        private final Function<AmazonDynamoDB, MtAmazonDynamoDb> buildClientFromRoot;
        private final boolean useMultitenantTables;

        AmazonDynamoDbStrategy(Function<AmazonDynamoDB, MtAmazonDynamoDb> buildClientFromRoot) {
            this(buildClientFromRoot, false);
        }

        AmazonDynamoDbStrategy(Function<AmazonDynamoDB, MtAmazonDynamoDb> buildClientFromRoot,
                               boolean useMultitenantTables) {
            this.buildClientFromRoot = buildClientFromRoot;
            this.useMultitenantTables = useMultitenantTables;
        }

        MtAmazonDynamoDb buildAmazonDynamoDb(AmazonDynamoDB rootAmazonDynamoDb) {
            return buildClientFromRoot.apply(rootAmazonDynamoDb);
        }

        boolean useMultitenantTables() {
            return useMultitenantTables;
        }
    }

    private AmazonDynamoDB rootAmazonDynamoDb = DEFAULT_ROOT_AMAZON_DYNAMO_DB;

    private List<AmazonDynamoDbStrategy> strategies = ImmutableList.of(
        /*
         * Testing byAccount by itself and with byTable succeeds, but SQLite failures occur when it runs
         * concurrently with any of the sharedTable* strategies.
         */
        //AmazonDynamoDbStrategy..ByAccount,
        AmazonDynamoDbStrategy.ByTable,
        AmazonDynamoDbStrategy.RandomPartitioning,
        AmazonDynamoDbStrategy.RandomPartitioningBinaryHk,
        AmazonDynamoDbStrategy.HashPartitioning,
        AmazonDynamoDbStrategy.HashPartitioningMt
    );

    public ArgumentBuilder withAmazonDynamoDb(AmazonDynamoDB rootAmazonDynamoDb) {
        this.rootAmazonDynamoDb = rootAmazonDynamoDb;
        return this;
    }

    public ArgumentBuilder withStrategies(List<AmazonDynamoDbStrategy> strategies) {
        this.strategies = strategies;
        return this;
    }

    @Override
    public List<TestArgument> get() {
        List<TestArgument> ret = new ArrayList<>();
        for (AmazonDynamoDbStrategy mtStrategy : getAmazonDynamoDbStrategies()) {
            for (ScalarAttributeType hashKeyAttributes : getHashKeyAttrTypes()) {
                ret.add(new TestArgument(mtStrategy, getOrgs(), hashKeyAttributes, rootAmazonDynamoDb));
            }
        }
        return ret;
    }

    /**
     * Returns a list of orgs to be used for a test.
     */
    private List<String> getOrgs() {
        return IntStream.rangeClosed(1, ORGS_PER_TEST).mapToObj(i -> "Org-" + ORG_COUNTER.incrementAndGet()).collect(
            Collectors.toList());
    }

    /**
     * Returns a list of DynamoDB data types to be used as the table's HASH key data type when creating virtual tables.
     */
    private List<ScalarAttributeType> getHashKeyAttrTypes() {
        return Arrays.stream(ScalarAttributeType.values()).collect(Collectors.toList());
    }


    /**
     * Returns a list of AmazonDynamoDB instances to be tested.
     */
    private List<AmazonDynamoDbStrategy> getAmazonDynamoDbStrategies() {
        return strategies;
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

        private final AmazonDynamoDbStrategy amazonDynamoDbStrategy;
        private final MtAmazonDynamoDb amazonDynamoDb;
        private final AmazonDynamoDB rootAmazonDynamoDb;
        private final List<String> orgs;
        private final ScalarAttributeType hashKeyAttrType;

        /**
         * Takes the arguments that make up the inputs to a test invocation.
         */
        TestArgument(AmazonDynamoDbStrategy amazonDynamoDbStrategy, List<String> orgs,
                     ScalarAttributeType hashKeyAttrType, AmazonDynamoDB rootAmazonDynamoDb) {
            this.amazonDynamoDbStrategy = amazonDynamoDbStrategy;
            this.amazonDynamoDb = amazonDynamoDbStrategy.buildAmazonDynamoDb(rootAmazonDynamoDb);
            this.orgs = orgs;
            this.hashKeyAttrType = hashKeyAttrType;
            this.rootAmazonDynamoDb = rootAmazonDynamoDb;
        }

        public AmazonDynamoDbStrategy getAmazonDynamoDbStrategy() {
            return amazonDynamoDbStrategy;
        }

        public MtAmazonDynamoDb getAmazonDynamoDb() {
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
            return amazonDynamoDbStrategy.name()
                + ", orgs=" + orgs
                + ", hashKeyAttrType=" + hashKeyAttrType.name();
        }
    }

}