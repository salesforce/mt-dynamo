package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder.SharedTableCreateTableRequestFactory;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMappingFactory;
import com.salesforce.dynamodbv2.mt.repo.MtDynamoDbTableDescriptionRepo;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides a means to provide a custom mapping of virtual to physical tables while falling back to the SharedTable
 * implementation for virtual tables that you don't provide a mapping for.
 *
 * @author msgroi
 */
public class HybridSharedTableBuilder {

    private MtAmazonDynamoDbContextProvider mtContext;
    private AmazonDynamoDB amazonDynamoDb;
    private boolean streamsEnabled = false;
    private long provisionedThroughput = 1L;
    private Optional<String> tablePrefix = Optional.empty();
    private int pollIntervalSeconds = 0;
    private CreateTableRequestFactory primaryCreateTableRequestFactory;

    public static HybridSharedTableBuilder builder() {
        return new HybridSharedTableBuilder();
    }

    /**
     * Builds an {@code MtAmazonDynamoDbBySharedTable}.
     *
     * @return a newly created {@code MtAmazonDynamoDbBySharedTable} based on the contents of the
     *     {@code HybridSharedTableBuilder}
     */
    public MtAmazonDynamoDbBySharedTable build() {
        CreateTableRequestFactoryEnsemble iteratingCreateTableRequestFactory =
            new CreateTableRequestFactoryEnsemble(ImmutableList.of(
                primaryCreateTableRequestFactory,
                new SharedTableCreateTableRequestFactory(
                    SharedTableBuilder.buildDefaultCreateTableRequests(provisionedThroughput, streamsEnabled),
                    tablePrefix)
            ));

        return new MtAmazonDynamoDbBySharedTable("MtAmazonDynamoDbBySharedTable:HybridSharedTableBuilder",
            mtContext,
            amazonDynamoDb,
            new TableMappingFactory(
                iteratingCreateTableRequestFactory,
                mtContext,
                new DynamoSecondaryIndexMapperByTypeImpl(),
                ".",
                amazonDynamoDb,
                true,
                pollIntervalSeconds),
            MtDynamoDbTableDescriptionRepo.builder()
                .withAmazonDynamoDb(amazonDynamoDb)
                .withContext(mtContext)
                .withTableDescriptionTableName("_tablemetadata")
                .withPollIntervalSeconds(pollIntervalSeconds)
                .withTablePrefix(tablePrefix).build(),
            true,
            false);
    }

    public HybridSharedTableBuilder withAmazonDynamoDb(AmazonDynamoDB amazonDynamoDb) {
        this.amazonDynamoDb = amazonDynamoDb;
        return this;
    }

    public HybridSharedTableBuilder withContext(MtAmazonDynamoDbContextProvider mtContext) {
        this.mtContext = mtContext;
        return this;
    }

    public HybridSharedTableBuilder withPrimaryCreateTableRequestFactory(CreateTableRequestFactory
        primaryCreateTableRequestFactory) {
        this.primaryCreateTableRequestFactory = primaryCreateTableRequestFactory;
        return this;
    }

    public HybridSharedTableBuilder withStreamsEnabled(boolean streamsEnabled) {
        this.streamsEnabled = streamsEnabled;
        return this;
    }

    public HybridSharedTableBuilder withProvisionedThroughput(long provisionedThroughput) {
        this.provisionedThroughput = provisionedThroughput;
        return this;
    }

    public HybridSharedTableBuilder withTablePrefix(String tablePrefix) {
        this.tablePrefix = Optional.of(tablePrefix);
        return this;
    }

    public HybridSharedTableBuilder withPollIntervalSeconds(Integer pollIntervalSeconds) {
        this.pollIntervalSeconds = pollIntervalSeconds;
        return this;
    }

    @VisibleForTesting
    static class CreateTableRequestFactoryEnsemble implements CreateTableRequestFactory {

        private final List<CreateTableRequestFactory> createTableRequestFactories;

        CreateTableRequestFactoryEnsemble(List<CreateTableRequestFactory> createTableRequestFactories) {
            this.createTableRequestFactories = createTableRequestFactories;
        }

        /**
         * Iterate through each CreateTableRequestFactory.  If a CreateTableRequestFactory returns a CreateTableRequest,
         * then return it.  Otherwise, if no CreateTableRequestFactory return a CreateTableRequest, then return
         * Optional.empty().
         */
        @Override
        public Optional<CreateTableRequest> getCreateTableRequest(DynamoTableDescription virtualTableDescription) {
            for (CreateTableRequestFactory createTableRequestFactory : createTableRequestFactories) {
                Optional<CreateTableRequest> createTableRequestOpt =
                    createTableRequestFactory.getCreateTableRequest(virtualTableDescription);
                if (createTableRequestOpt.isPresent()) {
                    return createTableRequestOpt;
                }
            }
            return Optional.empty();
        }

        @Override
        public List<CreateTableRequest> getPhysicalTables() {
            return createTableRequestFactories.stream().flatMap(
                (Function<CreateTableRequestFactory, Stream<CreateTableRequest>>) createTableRequestFactory ->
                    createTableRequestFactory.getPhysicalTables().stream()).collect(Collectors.toList());
        }

    }

}