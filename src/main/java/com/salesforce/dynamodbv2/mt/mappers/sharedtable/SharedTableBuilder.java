/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.backups.MtBackupTableSnapshotter;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.TableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType;
import com.salesforce.dynamodbv2.mt.mappers.index.HasPrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapper;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningStrategy;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtSharedTableBackupManagerBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.PhysicalTableManager;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.RandomPartitioningStrategy;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMapping;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMappingFactory;
import com.salesforce.dynamodbv2.mt.repo.MtDynamoDbTableDescriptionRepo;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Maps virtual tables to a set of 7 physical tables hard-coded into the builder by comparing the types of the elements
 * of the virtual table's primary key against the corresponding types on the physical tables.  It support mapping many
 * virtual tables to a single physical table, mapping field names and types, secondary indexes.  It supports for
 * allowing multitenant context to be added to table and index hash key fields.  Throughout this documentation, virtual
 * tables are meant to represent tables as they are understood by the developer using the DynamoDB Java API
 * (`AmazonDynamoDB`).  Physical tables represent the tables that store the data in AWS.  The implementation supports
 * virtual tables with up to 4 GSIs, where no more than one GSI hash/range key on a given virtual table may match one of
 * the following combinations: S(hk only), S-S, S-N, S-B.  It also supports up to 4 LSIs with the same limitation.
 *
 * <p>Below is are the physical tables that are created.  Virtual tables with no LSI will be mapped to the *_no_lsi
 * tables and won't be subject to the 10GB table size limit.  Otherwise, virtual tables are mapped to their physical
 * counterpart based on the rules described in {@code PrimaryKeyMapperByTypeImpl}.
 *
 * <p>All table names are prefixed with 'mt_shared_table_static_'.
 *
 * <p>TABLE NAME   s_s       s_n       s_b       s_no_lsi   s_s_no_lsi s_n_no_lsi s_b_no_lsi
 * -----------  --------- --------- --------- --------- --------- --------- ---------
 * table hash   S         S         S         S         S         S         S
 * range        S         N         B         -         S         N         B
 * gsi 1 hash   S         S         S         S         S         S         S
 * gsi 1 range  S         S         S         S         S         S         S
 * gsi 2 hash   S         S         S         S         S         S         S
 * gsi 2 range  N         N         N         N         N         N         N
 * gsi 3 hash   S         S         S         S         S         S         S
 * gsi 3 range  B         B         B         B         B         B         B
 * gsi 4 hash   S         S         S         S         S         S         S
 * gsi 4 range  -         -         -         -         -         -         -
 * lsi 1 hash   S         S         S
 * lsi 1 range  S         S         S
 * lsi 2 hash   S         S         S
 * lsi 2 range  N         N         N
 * lsi 3 hash   S         S         S
 * lsi 3 range  B         B         B
 *
 * <p>The builder requires ...
 *
 * <p>- an {@code AmazonDynamoDB} instance
 * - a multitenant context
 *
 * <p>Optionally ...
 *
 * <p>- {@code DynamoSecondaryIndexMapper}: Allows customization of mapping of virtual to physical
 *   secondary indexes.  Two implementations are provided, {@code DynamoSecondaryIndexMapperByNameImpl} and
 *   {@code DynamoSecondaryIndexMapperByTypeImpl}.  See Javadoc there for details.
 *   Default: {@code DynamoSecondaryIndexMapperByNameImpl}.
 * - {@code delimiter}: a {@code String} delimiter used to separate the tenant identifier prefix from the hash-key
 *   value.  Default: '-'.
 * - {@code tablePrefix}: a {@code String} used to prefix all tables with, independently of multitenant context, to
 *   provide the ability to support multiple environments within an account.
 * - {@code MtTableDescriptionRepo}: responsible for storing and retrieving table descriptions.
 *   Default: {@code MtDynamoDbTableDescriptionRepo}
 *   which stores table definitions in DynamoDB itself.
 * - {@code deleteTableAsync}: a {@code boolean} to indicate whether table data deletion may happen asynchronously after
 *   the table is dropped.  Default: FALSE.
 * - {@code truncateOnDeleteTable}: a {@code boolean} to indicate whether all of a table's data should be deleted when a
 *   table is dropped.  Default: FALSE.
 * - {@code createTablesEagerly}: a {@code boolean} to indicate whether the physical tables should be created eagerly.
 *   Default: TRUE.
 * - {@code tableMappingFactory}: the {@code TableMappingFactory} that maps virtual to physical table instances.
 *   Default: a table mapping factory that implements shared table behavior.
 * - {@code name}: a {@code String} representing the name of the multitenant AmazonDynamoDB instance.
 *   Default: "MtAmazonDynamoDbBySharedTable".
 * - {@code pollIntervalSeconds}: an {@code Integer} representing the interval in seconds between attempts at checking
 *   the status of the table being created.  Default: 0.
 *
 * <p>Limitations ...
 *
 * <p>- Supported methods: create|describe|delete* Table, get|put|update** Item, query***, scan***
 * - Drop Tables: When dropping a table, if you don't explicitly specify `truncateOnDeleteTable=true`, then table
 * data will be left behind even after the table is dropped.  If a table with the same name is later recreated under
 * the same tenant identifier, the data will be restored.  Note that undetermined behavior should be expected in the
 * event that the original table schema is different from the new table schema.
 * - Adding/removing `GSI`s/`LSI`s:  Adding or removing `GSI`s or `LSI`s on a table that contains data will cause
 * queries and scans to yield unexpected results.
 * - Projections in all `query` and `scan` requests default to `ProjectionType.ALL`.
 * - Deleting and recreating tables without deleting all table data(see truncateOnDeleteTable) may yield
 * unexpected results.
 *
 * <p>* See deleteTableAsync and truncateOnDeleteTable for details on how to
 * control behavior that is specific to deleteTable.
 * ** Updates on gsi hash keys are unsupported.  Performing updates via `UpdateItemRequest` objects
 * `withAttributeUpdates` and `addAttributeUpdateEntry` is not supported since they are considered 'legacy parameters'
 * according DynamoDB docs.  Standard update expressions are supported.
 * *** Only EQ, GT, GE, LT, and LE conditions are supported; GT, GE, LT, and LE via KeyConditions only
 *
 * <p>Design constraints:
 *
 * <p>- In order to support multitenancy, all HKs (table and index-level) must be prefixed with the alphanumeric
 * tenant ID.
 *   Therefore, all HKs must be of type S.
 * - Tables with LSIs are limited to 10GB
 *   (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html#LSI.ItemCollections.SizeLimit).
 *   Therefore, we have two sets of tables, one set with LSIs and one set without.
 * - Tables with HK only may not have LSIs (error: "AmazonServiceException: Local Secondary indices are not allowed on
 *   hash tables, only hash and range tables").  Therefore, the only table that has HK only does not have an LSI.
 * - Virtual tables with only a HK may not be mapped to a table that has both a HK and RK per
 *   https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html, "When you add an item, the primary
 *   key attribute(s) are the only required attributes.  Attribute values cannot be null."  See
 *   {@code SharedTableRangeKeyTest} for a simple test that demonstrates this.
 * - All virtual types could have been mapped into a set of tables that have only byte array types, and convert
 *   all virtual types down to byte arrays and back.  This would necessitate a smaller set of tables, possibly as few
 *   as 3.  However, the mapping layer would also need to be responsible for maintaining consistency with respect to
 *   sorting so it was not implemented.
 *
 */
public class SharedTableBuilder implements TableBuilder {

    private static final String DEFAULT_TABLE_DESCRIPTION_TABLE_NAME = "_table_metadata";

    /**
     * Special default "column" key returned to client on multitenant scans. Configurable by clients if needed.
     */
    private static final String DEFAULT_SCAN_TENANT_KEY = "mt:context";
    private static final String DEFAULT_SCAN_VIRTUAL_TABLE_KEY = "mt:tableName";


    /**
     * Special default prefix to a physical table name use for temp snapshots of tables to generate tenant backups.
     */
    private static final String DEFAULT_BACKUP_TABLE_PREFIX = "mt-table-snapshot-";

    private CreateTableRequestFactory createTableRequestFactory;
    private List<CreateTableRequest> createTableRequests;
    private Boolean canCreatePhysicalTables = Boolean.TRUE;

    private Long defaultProvisionedThroughput; /* TODO if this is ever going to be used in production we will need
                                                       more granularity, like at the table, index, read, write level */

    private BillingMode billingMode;
    private Boolean streamsEnabled;
    private String name;
    private AmazonDynamoDB amazonDynamoDb;
    private MtAmazonDynamoDbContextProvider mtContext;
    private MtTableDescriptionRepo mtTableDescriptionRepo;
    private TableMappingFactory tableMappingFactory;
    private MtSharedTableBackupManagerBuilder backupManagerBuilder;
    private Boolean binaryHashKey;
    private Boolean deleteTableAsync;
    private Boolean truncateOnDeleteTable;
    private Boolean createTablesEagerly;
    private Integer pollIntervalSeconds;
    private Optional<String> tablePrefix = empty();
    private Long getRecordsTimeLimit;
    private Clock clock;
    private String tableDescriptionTableName;
    private Cache<Object, Optional<TableMapping>> tableMappingCache;
    private Cache<Object, TableDescription> tableDescriptionCache;
    private MeterRegistry meterRegistry;
    private String scanTenantKey = DEFAULT_SCAN_TENANT_KEY;
    private String scanVirtualTableKey = DEFAULT_SCAN_VIRTUAL_TABLE_KEY;
    private String backupTablePrefix = DEFAULT_BACKUP_TABLE_PREFIX;
    private TablePartitioningStrategy partitioningStrategy;

    public static SharedTableBuilder builder() {
        return new SharedTableBuilder();
    }

    private static String prefix(Optional<String> tablePrefix, String tableName) {
        return tablePrefix.map(tablePrefix1 -> tablePrefix1 + tableName).orElse(tableName);
    }

    public SharedTableBuilder withCanCreatePhysicalTables(boolean canCreatePhysicalTables) {
        this.canCreatePhysicalTables = canCreatePhysicalTables;
        return this;
    }

    /**
     * Sets the {@link CreateTableRequestFactory} that decides how to map each virtual table to a physical table.
     * <p/>
     * NOTE: Can't be used if {@link #withCreateTableRequests} is also used.
     */
    public SharedTableBuilder withCreateTableRequestFactory(CreateTableRequestFactory createTableRequestFactory) {
        checkState(createTableRequests == null,
            "Cannot specify CreateTableRequestFactory when CreateTableRequests are already specified");
        this.createTableRequestFactory = createTableRequestFactory;
        return this;
    }

    /**
     * Sets the {@link CreateTableRequest}s representing the physical tables that virtual tables can be mapped to.
     * <p/>
     * NOTE: Can't be used if {@link #withCreateTableRequestFactory} is also used.
     */
    public SharedTableBuilder withCreateTableRequests(CreateTableRequest... createTableRequests) {
        checkState(createTableRequestFactory == null,
            "Cannot specify CreateTableRequests when CreateTableRequestFactory is already specified");
        if (this.createTableRequests == null) {
            this.createTableRequests = new ArrayList<>();
        }
        this.createTableRequests.addAll(Arrays.asList(createTableRequests));
        return this;
    }

    public SharedTableBuilder withStreamsEnabled(boolean streamsEnabled) {
        this.streamsEnabled = streamsEnabled;
        return this;
    }

    public SharedTableBuilder withGetRecordsTimeLimit(long getRecordsTimeLimit) {
        this.getRecordsTimeLimit = getRecordsTimeLimit;
        return this;
    }

    public SharedTableBuilder withBackupSupport(AmazonS3 s3Client, String backupS3BucketName) {
        return this.withBackupSupport(s3Client, backupS3BucketName,
            new MtBackupTableSnapshotter());
    }

    public SharedTableBuilder withBackupSupport(AmazonS3 s3Client,
                                                String backupS3BucketName,
                                                MtBackupTableSnapshotter tableSnapshotter) {
        this.backupManagerBuilder = new MtSharedTableBackupManagerBuilder(s3Client, backupS3BucketName,
            tableSnapshotter);
        return this;
    }

    public SharedTableBuilder withClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    public SharedTableBuilder withDefaultProvisionedThroughput(long defaultProvisionedThroughput) {
        this.defaultProvisionedThroughput = defaultProvisionedThroughput;
        return this;
    }

    @Override
    public SharedTableBuilder withBillingMode(BillingMode billingMode) {
        this.billingMode = billingMode;
        return this;
    }

    @Override
    public SharedTableBuilder withScanTenantKey(String scanTenantKey) {
        this.scanTenantKey = scanTenantKey;
        return this;
    }

    @Override
    public SharedTableBuilder withScanVirtualTableKey(String scanVirtualTableKey) {
        this.scanVirtualTableKey = scanVirtualTableKey;
        return this;
    }

    public SharedTableBuilder withBackupTablePrefix(String backupPrefix) {
        Preconditions.checkNotNull(backupPrefix);
        this.backupTablePrefix = backupPrefix;
        return this;
    }

    public SharedTableBuilder withBinaryHashKey(boolean binaryHashKey) {
        this.binaryHashKey = binaryHashKey;
        return this;
    }

    public SharedTableBuilder withTableDescriptionTableName(String tableDescriptionTableName) {
        this.tableDescriptionTableName = tableDescriptionTableName;
        return this;
    }

    public SharedTableBuilder withMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        return this;
    }

    public SharedTableBuilder withPartitioningStrategy(TablePartitioningStrategy strategy) {
        this.partitioningStrategy = strategy;
        return this;
    }

    /**
     * TODO: write Javadoc.
     *
     * @return a newly created {@code MtAmazonDynamoDbBySharedTable} based on the contents of the
     * {@code SharedTableBuilder}
     */
    public MtAmazonDynamoDbBySharedTable build() {
        setDefaults();
        withName("SharedTableBuilder");
        validate();
        final PhysicalTableManager physicalTableManager = new PhysicalTableManager(amazonDynamoDb, pollIntervalSeconds,
            canCreatePhysicalTables, createTablesEagerly ? createTableRequests : Collections.emptyList());
        if (tableMappingFactory == null) {
            tableMappingFactory = new TableMappingFactory(
                createTableRequestFactory,
                partitioningStrategy,
                physicalTableManager
            );
        }
        return new MtAmazonDynamoDbBySharedTable(name,
            mtContext,
            amazonDynamoDb,
            tableMappingFactory,
            partitioningStrategy,
            mtTableDescriptionRepo,
            physicalTableManager,
            deleteTableAsync,
            truncateOnDeleteTable,
            getRecordsTimeLimit,
            clock,
            tableMappingCache,
            meterRegistry,
            Optional.ofNullable(backupManagerBuilder),
            scanTenantKey,
            scanVirtualTableKey,
            backupTablePrefix);
    }

    private void setDefaults() {
        if (this.defaultProvisionedThroughput == null) {
            this.defaultProvisionedThroughput = 1L;
        }
        if (this.billingMode == null) {
            this.billingMode = BillingMode.PROVISIONED;
        }
        if (this.streamsEnabled == null) {
            streamsEnabled = true;
        }
        if (this.binaryHashKey == null) {
            binaryHashKey = false;
        }
        if (partitioningStrategy == null) {
            partitioningStrategy = new RandomPartitioningStrategy();
        }
        if (createTableRequestFactory == null) {
            if (this.createTableRequests == null || this.createTableRequests.isEmpty()) {
                this.createTableRequests = buildDefaultCreateTableRequests(this.defaultProvisionedThroughput,
                    this.billingMode, this.streamsEnabled, this.binaryHashKey, this.partitioningStrategy);
            } else if (this.billingMode.equals(BillingMode.PAY_PER_REQUEST)) {
                this.createTableRequests = createTableRequests.stream()
                    .map(createTableRequest ->
                        createTableRequest.withBillingMode(BillingMode.PAY_PER_REQUEST))
                    .collect(Collectors.toList());
            }
            createTableRequestFactory = new StaticCreateTableRequestFactory(
                partitioningStrategy.getTablePrimaryKeyMapper(), createTableRequests, getTablePrefix());
        }
        if (name == null) {
            name = "MtAmazonDynamoDbBySharedTable";
        }
        if (truncateOnDeleteTable == null) {
            truncateOnDeleteTable = false;
        }
        if (deleteTableAsync == null) {
            deleteTableAsync = false;
        }
        if (canCreatePhysicalTables == null) {
            canCreatePhysicalTables = true;
        }
        if (createTablesEagerly == null && canCreatePhysicalTables && createTableRequests != null) {
            createTablesEagerly = true;
        }
        if (pollIntervalSeconds == null) {
            pollIntervalSeconds = 0;
        }
        if (tableDescriptionTableName == null) {
            tableDescriptionTableName = DEFAULT_TABLE_DESCRIPTION_TABLE_NAME;
        }
        if (tableDescriptionCache == null) {
            tableDescriptionCache = CacheBuilder.newBuilder().build();
        }
        if (tableMappingCache == null) {
            tableMappingCache = CacheBuilder.newBuilder().build();
        }
        if (mtTableDescriptionRepo == null) {
            mtTableDescriptionRepo = MtDynamoDbTableDescriptionRepo.builder()
                .withAmazonDynamoDb(amazonDynamoDb)
                .withBillingMode(this.billingMode)
                .withContext(mtContext)
                .withTableDescriptionTableName(tableDescriptionTableName)
                .withPollIntervalSeconds(pollIntervalSeconds)
                .withTablePrefix(tablePrefix)
                .withTableDescriptionCache(tableDescriptionCache)
                .build();

            ((MtDynamoDbTableDescriptionRepo) mtTableDescriptionRepo)
                .createDefaultDescriptionTable();
        }
        if (getRecordsTimeLimit == null) {
            getRecordsTimeLimit = 5000L;
        }
        if (clock == null) {
            clock = Clock.systemDefaultZone();
        }
        if (meterRegistry == null) {
            meterRegistry = new CompositeMeterRegistry();
        }
    }

    private static final String HASH_KEY_FIELD = "hk";
    private static final String RANGE_KEY_FIELD = "rk";

    /**
     * Builds the tables underlying the SharedTable implementation as described in the class-level Javadoc.
     */
    private static List<CreateTableRequest> buildDefaultCreateTableRequests(
        long provisionedThroughput, BillingMode billingMode, boolean streamsEnabled, boolean binaryHashKey,
        TablePartitioningStrategy partitioningStrategy) {

        if (partitioningStrategy instanceof HashPartitioningStrategy) {
            checkArgument(binaryHashKey, "Table hash key must be binary when using hash partitioning");
            int numGsis = 5;
            CreateTableRequestBuilder noLsi = CreateTableRequestBuilder.builder()
                .withTableName("mt_shared_table")
                .withTableKeySchema(HASH_KEY_FIELD, B, RANGE_KEY_FIELD, B);
            for (int i = 0; i < numGsis; i++) {
                String name = "gsi" + i;
                noLsi.addSi(name, GSI, new PrimaryKey(name + "_hk", B, name + "_rk", B), provisionedThroughput);
            }
            CreateTableRequestBuilder withLsi = CreateTableRequestBuilder.builder()
                .withTableName("mt_shared_table_lsi")
                .withTableKeySchema(HASH_KEY_FIELD, B, RANGE_KEY_FIELD, B)
                .addSi("lsi", LSI, new PrimaryKey("hk", B, "lsi_rk", B), provisionedThroughput);
            for (int i = 0; i < numGsis; i++) {
                String name = "gsi" + i;
                withLsi.addSi(name, GSI, new PrimaryKey(name + "_hk", B, name + "_rk", B), provisionedThroughput);
            }
            return ImmutableList.of(noLsi, withLsi).stream()
                .map(createTableRequestBuilder -> {
                    setBillingMode(createTableRequestBuilder, billingMode, provisionedThroughput);
                    addStreamSpecification(createTableRequestBuilder, streamsEnabled);
                    return createTableRequestBuilder.build();
                }).collect(Collectors.toList());
        } else {
            ScalarAttributeType hashKeyType = binaryHashKey ? B : S;

            CreateTableRequestBuilder mtSharedTableStaticSs = CreateTableRequestBuilder.builder()
                .withTableName("mt_shared_table_static_" + hashKeyType.name().toLowerCase() + "_s")
                .withTableKeySchema(HASH_KEY_FIELD, hashKeyType, RANGE_KEY_FIELD, S);
            CreateTableRequestBuilder mtSharedTableStaticSn = CreateTableRequestBuilder.builder()
                .withTableName("mt_shared_table_static_" + hashKeyType.name().toLowerCase() + "_n")
                .withTableKeySchema(HASH_KEY_FIELD, hashKeyType, RANGE_KEY_FIELD, N);
            CreateTableRequestBuilder mtSharedTableStaticSb = CreateTableRequestBuilder.builder()
                .withTableName("mt_shared_table_static_" + hashKeyType.name().toLowerCase() + "_b")
                .withTableKeySchema(HASH_KEY_FIELD, hashKeyType, RANGE_KEY_FIELD, B);
            CreateTableRequestBuilder mtSharedTableStaticsNoLsi = CreateTableRequestBuilder.builder()
                .withTableName("mt_shared_table_static_" + hashKeyType.name().toLowerCase() + "_no_lsi")
                .withTableKeySchema(HASH_KEY_FIELD, hashKeyType);
            CreateTableRequestBuilder mtSharedTableStaticSsNoLsi = CreateTableRequestBuilder.builder()
                .withTableName("mt_shared_table_static_" + hashKeyType.name().toLowerCase() + "_s_no_lsi")
                .withTableKeySchema(HASH_KEY_FIELD, hashKeyType, RANGE_KEY_FIELD, S);
            CreateTableRequestBuilder mtSharedTableStaticSnNoLsi = CreateTableRequestBuilder.builder()
                .withTableName("mt_shared_table_static_" + hashKeyType.name().toLowerCase() + "_n_no_lsi")
                .withTableKeySchema(HASH_KEY_FIELD, hashKeyType, RANGE_KEY_FIELD, N);
            CreateTableRequestBuilder mtSharedTableStaticSbNoLsi = CreateTableRequestBuilder.builder()
                .withTableName("mt_shared_table_static_" + hashKeyType.name().toLowerCase() + "_b_no_lsi")
                .withTableKeySchema(HASH_KEY_FIELD, hashKeyType, RANGE_KEY_FIELD, B);

            return ImmutableList.of(mtSharedTableStaticSs,
                mtSharedTableStaticSn,
                mtSharedTableStaticSb,
                mtSharedTableStaticsNoLsi,
                mtSharedTableStaticSsNoLsi,
                mtSharedTableStaticSnNoLsi,
                mtSharedTableStaticSbNoLsi
            ).stream().map(createTableRequestBuilder -> {
                setBillingMode(createTableRequestBuilder, billingMode, provisionedThroughput);
                addSis(createTableRequestBuilder, hashKeyType, provisionedThroughput);
                addStreamSpecification(createTableRequestBuilder, streamsEnabled);
                return createTableRequestBuilder.build();
            }).collect(Collectors.toList());
        }
    }

    /**
     * Set billing mode based on input throughput.
     *
     * @param createTableRequestBuilder the {@code CreateTableRequestBuilder} defines the table creation definition
     * @param provisionedThroughput     the throughput to assign to the request (if 0, billing mode is set to PPR)
     */
    private static void setBillingMode(CreateTableRequestBuilder createTableRequestBuilder, BillingMode billingMode,
                                       long provisionedThroughput) {

        if (billingMode != null && billingMode.equals(BillingMode.PAY_PER_REQUEST)) {
            createTableRequestBuilder.withBillingMode(BillingMode.PAY_PER_REQUEST);
        } else {
            createTableRequestBuilder.withBillingMode(billingMode);
            createTableRequestBuilder.withProvisionedThroughput(provisionedThroughput, provisionedThroughput);
        }
    }

    private static void addSis(CreateTableRequestBuilder createTableRequestBuilder, ScalarAttributeType hashKeyType,
                               long defaultProvisionedThroughput) {
        addSi(createTableRequestBuilder, GSI, hashKeyType, empty(), defaultProvisionedThroughput);
        addSi(createTableRequestBuilder, GSI, hashKeyType, of(S), defaultProvisionedThroughput);
        addSi(createTableRequestBuilder, GSI, hashKeyType, of(N), defaultProvisionedThroughput);
        addSi(createTableRequestBuilder, GSI, hashKeyType, of(B), defaultProvisionedThroughput);
        if (!createTableRequestBuilder.getTableName().toLowerCase().endsWith("no_lsi")) {
            addSi(createTableRequestBuilder, LSI, hashKeyType, of(S), defaultProvisionedThroughput);
            addSi(createTableRequestBuilder, LSI, hashKeyType, of(N), defaultProvisionedThroughput);
            addSi(createTableRequestBuilder, LSI, hashKeyType, of(B), defaultProvisionedThroughput);
        }
    }

    private static void addStreamSpecification(CreateTableRequestBuilder createTableRequestBuilder,
                                               boolean streamsEnabled) {
        createTableRequestBuilder.withStreamSpecification(streamsEnabled
            ? new StreamSpecification().withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
            .withStreamEnabled(true)
            : new StreamSpecification().withStreamEnabled(false));
    }

    private static void addSi(CreateTableRequestBuilder createTableRequestBuilder,
                              DynamoSecondaryIndexType indexType,
                              ScalarAttributeType hashKeyType,
                              Optional<ScalarAttributeType> rangeKeyType,
                              long defaultProvisionedThroughput) {
        String indexName = indexType.name().toLowerCase() + "_"
            + hashKeyType.name().toLowerCase()
            + rangeKeyType.map(type -> "_" + type.name().toLowerCase()).orElse("").toLowerCase();
        PrimaryKey primaryKey = rangeKeyType.map(
            scalarAttributeType -> new PrimaryKey(indexType == LSI ? "hk" : indexName + "_hk",
                hashKeyType,
                indexName + "_rk",
                scalarAttributeType))
            .orElseGet(() -> new PrimaryKey(indexName + "_hk",
                hashKeyType));
        createTableRequestBuilder.addSi(indexName,
            indexType,
            primaryKey,
            defaultProvisionedThroughput);
    }

    public SharedTableBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public SharedTableBuilder withAmazonDynamoDb(AmazonDynamoDB amazonDynamoDb) {
        this.amazonDynamoDb = amazonDynamoDb;
        return this;
    }

    public SharedTableBuilder withContext(MtAmazonDynamoDbContextProvider mtContext) {
        this.mtContext = mtContext;
        return this;
    }

    public SharedTableBuilder withTablePrefix(String tablePrefix) {
        this.tablePrefix = of(tablePrefix);
        return this;
    }

    public SharedTableBuilder withTableDescriptionRepo(MtTableDescriptionRepo mtTableDescriptionRepo) {
        this.mtTableDescriptionRepo = mtTableDescriptionRepo;
        return this;
    }

    public SharedTableBuilder withDeleteTableAsync(boolean dropAsync) {
        deleteTableAsync = dropAsync;
        return this;
    }

    public SharedTableBuilder withTruncateOnDeleteTable(Boolean truncateOnDrop) {
        truncateOnDeleteTable = truncateOnDrop;
        return this;
    }

    public SharedTableBuilder withCreateTablesEagerly(boolean createTablesEagerly) {
        this.createTablesEagerly = createTablesEagerly;
        return this;
    }

    public SharedTableBuilder withPollIntervalSeconds(Integer pollIntervalSeconds) {
        this.pollIntervalSeconds = pollIntervalSeconds;
        return this;
    }

    public SharedTableBuilder withTableMappingCache(Cache<Object, Optional<TableMapping>> tableMappingCache) {
        this.tableMappingCache = tableMappingCache;
        return this;
    }

    public SharedTableBuilder withTableDescriptionCache(Cache<Object, TableDescription> tableDescriptionCache) {
        this.tableDescriptionCache = tableDescriptionCache;
        return this;
    }

    private Optional<String> getTablePrefix() {
        return tablePrefix;
    }

    private void validate() {
        checkNotNull(amazonDynamoDb, "amazonDynamoDb is required");
        checkNotNull(mtContext, "mtContext is required");

        if (createTableRequests != null) {
            boolean tableCollidesWithBackupPrefix = createTableRequests
                .stream()
                .map(CreateTableRequest::getTableName)
                .anyMatch(c -> c.startsWith(DEFAULT_BACKUP_TABLE_PREFIX));
            checkState(!tableCollidesWithBackupPrefix,
                "Cannot prefix a physical table name with "
                    + DEFAULT_BACKUP_TABLE_PREFIX
                    + ". Either change your table names or override the default backup suffix.");
        }
        if (createTablesEagerly) {
            checkState(canCreatePhysicalTables,
                "Cannot create physical tables eagerly when creating physical tables is not allowed");
            checkState(createTableRequests != null,
                "Cannot create physical tables eagerly when there isn't a static set of tables");
        }
    }

    private static class CreateTableRequestWrapper implements HasPrimaryKey {

        private final CreateTableRequest createTableRequest;

        CreateTableRequestWrapper(CreateTableRequest createTableRequest) {
            this.createTableRequest = createTableRequest;
        }

        @Override
        public PrimaryKey getPrimaryKey() {
            return new DynamoTableDescriptionImpl(createTableRequest).getPrimaryKey();
        }

        private CreateTableRequest getCreateTableRequest() {
            return createTableRequest;
        }

    }

    /**
     * Implements the request factory that is capable of mapping virtual tables to the physical tables underlying
     * the SharedTable multitenancy strategy as described in the class-level Javadoc.
     */
    private static class StaticCreateTableRequestFactory implements CreateTableRequestFactory {

        private final PrimaryKeyMapper primaryKeyMapper;
        private final List<CreateTableRequest> createTableRequests;
        private final Set<String> tableNames;

        StaticCreateTableRequestFactory(PrimaryKeyMapper primaryKeyMapper,
                                        List<CreateTableRequest> createTableRequests,
                                        Optional<String> tablePrefix) {
            this.primaryKeyMapper = primaryKeyMapper;
            this.createTableRequests = createTableRequests.stream()
                .map(createTableRequest -> createTableRequest.withTableName(
                    prefix(tablePrefix, createTableRequest.getTableName())))
                .collect(Collectors.toList());
            this.tableNames = this.createTableRequests.stream()
                .map(CreateTableRequest::getTableName)
                .collect(Collectors.toSet());
        }

        @Override
        public Optional<CreateTableRequest> getCreateTableRequest(DynamoTableDescription virtualTableDescription) {
            try {
                boolean hasLsis = !isEmpty(virtualTableDescription.getLsis());
                return Optional.of(((CreateTableRequestWrapper) primaryKeyMapper
                    .mapPrimaryKey(virtualTableDescription.getPrimaryKey(), createTableRequests.stream()
                        .filter(createTableRequest1 -> hasLsis
                            == !isEmpty(createTableRequest1.getLocalSecondaryIndexes()))
                        .map((Function<CreateTableRequest, HasPrimaryKey>) CreateTableRequestWrapper::new)
                        .collect(Collectors.toList())))
                    .getCreateTableRequest());
            } catch (MappingException e) {
                throw new RuntimeException(e);
            }
        }

        private boolean isEmpty(List<?> l) {
            return l == null || l.isEmpty();
        }

        @Override
        public boolean isPhysicalTable(String physicalTableName) {
            return tableNames.contains(physicalTableName);
        }

    }

}