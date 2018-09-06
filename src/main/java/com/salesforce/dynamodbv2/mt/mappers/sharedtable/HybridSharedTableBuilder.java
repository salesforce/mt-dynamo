package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder.SharedTableCreateTableRequestFactory;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMappingFactory;
import com.salesforce.dynamodbv2.mt.repo.MtDynamoDbTableDescriptionRepo;
import java.util.List;
import java.util.Map;
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
     * Builds a MtAmazonDynamoDbBySharedTable.
     */
    public MtAmazonDynamoDbBySharedTable build() {
        IteratingCreateTableRequestFactory iteratingCreateTableRequestFactory =
            new IteratingCreateTableRequestFactory(ImmutableList.of(
                primaryCreateTableRequestFactory,
                new SharedTableCreateTableRequestFactory(
                    SharedTableBuilder.buildDefaultCreateTableRequests(provisionedThroughput, streamsEnabled),
                    true,
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

    private Map<String, CreateTableRequest> virtualTableToCreateTableRequestMap() {
        return ImmutableMap.of(
            "Event", buildEventCreateTableRequest(),
            "Aggregate", buildAggregateCreateTableRequest());
    }

    private CreateTableRequest buildEventCreateTableRequest() {
        return new CreateTableRequest()
            .withTableName("Event")
            .withAttributeDefinitions(
                new AttributeDefinition("aggregate_id", ScalarAttributeType.S),
                new AttributeDefinition("sequence_number", ScalarAttributeType.N))
            .withKeySchema(new KeySchemaElement("aggregate_id", KeyType.HASH),
                new KeySchemaElement("sequence_number", KeyType.RANGE))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withStreamSpecification(new StreamSpecification()
                .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                .withStreamEnabled(true));
    }

    private CreateTableRequest buildAggregateCreateTableRequest() {
        return new CreateTableRequest()
            .withTableName("Aggregate")
            .withAttributeDefinitions(
                new AttributeDefinition("id", ScalarAttributeType.S))
            .withKeySchema(new KeySchemaElement("id", KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
            .withStreamSpecification(new StreamSpecification()
                .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                .withStreamEnabled(true));
    }

    private class IteratingCreateTableRequestFactory implements CreateTableRequestFactory {

        private final List<CreateTableRequestFactory> createTableRequestFactories;

        IteratingCreateTableRequestFactory(List<CreateTableRequestFactory> createTableRequestFactories) {
            this.createTableRequestFactories = createTableRequestFactories;
        }

        @Override
        public CreateTableRequest getCreateTableRequest(DynamoTableDescription virtualTableDescription) {
            return createTableRequestFactories.stream().flatMap(
                (Function<CreateTableRequestFactory, Stream<CreateTableRequest>>) createTableRequestFactory ->
                    Stream.of(createTableRequestFactory.getCreateTableRequest(virtualTableDescription)))
                .findFirst().orElseThrow(() ->
                    new IllegalArgumentException("no mapping found for " + virtualTableDescription.getTableName()));
        }

        @Override
        public List<CreateTableRequest> precreateTables() {
            return createTableRequestFactories.stream().flatMap(
                (Function<CreateTableRequestFactory, Stream<CreateTableRequest>>) createTableRequestFactory ->
                    createTableRequestFactory.precreateTables().stream()).collect(Collectors.toList());
        }

    }

}