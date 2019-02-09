package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import static com.amazonaws.services.dynamodbv2.model.BillingMode.PAY_PER_REQUEST;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.amazonaws.services.dynamodbv2.model.StreamViewType.NEW_AND_OLD_IMAGES;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapper;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.SaltedHashKeyTableMapping;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMapping;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMappingFactory;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class SaltedSharedTableBuilder extends SharedTableCustomDynamicBuilder {

    public static SaltedSharedTableBuilder builder() {
        return new SaltedSharedTableBuilder();
    }

    private static final String HASH_KEY_FIELD = "hk";
    private static final String RANGE_KEY_FIELD = "rk";
    private static final int NUM_GSIS = 5;
    private static final String GSI_NAME_PREFIX = "gsi";
    private static final String GSI_HASH_KEY_PREFIX = "hk_gsi";
    private static final String GSI_RANGE_KEY_PREFIX = "rk_gsi";

    private static final byte DEFAULT_NUM_SALT_BUCKETS = 1;

    private byte numSaltBuckets = DEFAULT_NUM_SALT_BUCKETS;

    private List<CreateTableRequest> createTableRequests;

    @Override
    public MtAmazonDynamoDbBySharedTable build() {
        super.setDefaults();

        List<CreateTableRequest> tables = buildDefaultCreateTableRequests();
        tableMappingFactory = new TableMappingFactory(
            new CreateTableRequestFactory() {
                @Override
                public Optional<CreateTableRequest> getCreateTableRequest(
                    DynamoTableDescription virtualTableDescription) {
                    return Optional.of(tables.get(0));
                }

                @Override
                public List<CreateTableRequest> getPhysicalTables() {
                    return tables;
                }
            },
            checkNotNull(mtContext, "mtContext is required"),
            new DynamoSecondaryIndexMapper() {
                @Override
                public DynamoSecondaryIndex lookupPhysicalSecondaryIndex(DynamoSecondaryIndex virtualSi,
                    DynamoTableDescription physicalTable) throws MappingException {
                    return null;
                }
            },
            delimiter,
            checkNotNull(amazonDynamoDb, "amazonDynamoDb is required"),
            precreateTables,
            pollIntervalSeconds
        ) {
            @Override
            protected TableMapping createTableMapping(DynamoTableDescription physicalTable,
                DynamoTableDescription virtualTable, DynamoSecondaryIndexMapper secondaryIndexMapper,
                MtAmazonDynamoDbContextProvider mtContext, char delimiter) {
                return new SaltedHashKeyTableMapping(mtContext, physicalTable, virtualTable, numSaltBuckets, delimiter);
            }
        };
        return new MtAmazonDynamoDbBySharedTable(name,
            mtContext,
            amazonDynamoDb,
            tableMappingFactory,
            mtTableDescriptionRepo,
            deleteTableAsync,
            truncateOnDeleteTable);
    }

    public SaltedSharedTableBuilder withNumSaltBuckets(byte numSaltBuckets) {
        this.numSaltBuckets = numSaltBuckets;
        return this;
    }

    protected void setDefaults() {
        if (this.createTableRequests == null || this.createTableRequests.isEmpty()) {
            this.createTableRequests = buildDefaultCreateTableRequests();
        }
        super.setDefaults();
    }

    private List<CreateTableRequest> buildDefaultCreateTableRequests() {
        CreateTableRequestBuilder mtSaltedSharedTable = CreateTableRequestBuilder.builder()
            .withTableName("mt_salted_shared_table")
            .withTableKeySchema(HASH_KEY_FIELD, S, RANGE_KEY_FIELD, B)
            .withBillingMode(PAY_PER_REQUEST)
            .withStreamSpecification(
                new StreamSpecification()
                    .withStreamViewType(NEW_AND_OLD_IMAGES)
                    .withStreamEnabled(true));
        for (int i = 0; i < NUM_GSIS; i++) {
            mtSaltedSharedTable.addSi(
                GSI_NAME_PREFIX + i,
                GSI,
                new PrimaryKey(GSI_HASH_KEY_PREFIX + i, S, GSI_RANGE_KEY_PREFIX + i, B),
                1L);
        }
        return Collections.singletonList(mtSaltedSharedTable.build());
    }

}
