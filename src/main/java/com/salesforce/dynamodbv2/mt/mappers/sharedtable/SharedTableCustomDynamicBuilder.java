/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapper;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperByNameImpl;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMappingFactory;
import com.salesforce.dynamodbv2.mt.repo.MtDynamoDbTableDescriptionRepo;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo;
import java.util.Optional;

/**
 * Allows a developer to control the mapping of virtual to physical tables by providing a CreateTableRequestFactory.
 * Hash keys in the physical table will be appropriately prefixed with tenant context, providing logical separation
 * of tenants even if they are mapped to the same physical table.
 *
 * <p>It also requires that the types of each element of the virtual table's primary key are compatible with that of
 * the physical table.
 *
 * <p>The default behavior if not overridden by providing an alternate DynamoSecondaryIndexMapper implementation
 * requires that each secondary index on the virtual table has a corresponding secondary index on the physical table
 * with the same name where types are compatible.
 *
 * <p>Table and Secondary Index Primary Key Compatibility
 *
 * <p>A virtual table's primary key or secondary index's primary key is considered compatible with a physical table's
 * primary key if the physical primary key has a hashkey of type S and either, range keys that are undefined on both
 * the virtual and physical tables or they are defined on both and have types that match.
 *
 * <p>The builder required ...
 *
 * <p>- A CreateTableRequestFactory implementation which allows the client to map virtual CreateTableRequest's to
 * physical CreateTableRequests.  By default, requests that reference secondary indexes will be mapped to their
 * counterpart on the virtual table using DynamoSecondaryIndexMapperByNameImpl, which expects that any secondary
 * index name referenced in the virtual table exists by name in the physical table.
 * - an AmazonDynamoDB instance
 * - a multi-tenant context
 *
 * <p>Optionally ...
 *
 * <p>- DynamoSecondaryIndexMapper: Allows customization of mapping of virtual to physical
 *   secondary indexes.  Two implementations are provided, DynamoSecondaryIndexMapperByNameImpl and
 *   DynamoSecondaryIndexMapperByTypeImpl.  See Javadoc there for details.
 *   Default: DynamoSecondaryIndexMapperByNameImpl.
 * - delimiter: a String delimiter used to separate the tenant identifier prefix from the hashkey value.  Default: '-'.
 *   See Javadoc below.  Default: true.
 * - tablePrefix: a String used to prefix all tables with, independently of multi-tenant context, to provide the
 *   ability to support multiple environments within an account.
 * - MtTableDescriptionRepo: Responsible for storing and retrieving table descriptions.
 *   Default: MtDynamoDbTableDescriptionRepo
 *   which stores table definitions in DynamoDB itself.
 * - deleteTableAsync: a boolean to indicate whether table data deletion may happen asynchronously after the table is
 *   dropped, default: FALSE
 * - truncateOnDeleteTable: a boolean to indicate whether all of a table's data should be deleted when a table is
 *   dropped, default: FALSE
 *
 * <p>Limitations ...
 *
 * <p>Supported methods: create|describe|delete* Table, get|put|update** Item, query***, scan***
 *
 * <p>* See deleteTableAsync and truncateOnDeleteTable in the SharedTableCustomDynamicBuilder for details on how to
 * control behavior that is specific to deleteTable.
 * ** Updates on gsi hashkey's are unsupported
 * *** Only EQ conditions are supported.
 *
 * <p>Deleting and recreating tables without deleting all table data(see truncateOnDeleteTable) may yield
 * unexpected results.
 */
public class SharedTableCustomDynamicBuilder {

    private static final String DEFAULT_TABLE_DESCRIPTION_TABLENAME = "_tablemetadata";
    private String name;
    private AmazonDynamoDB amazonDynamoDb;
    private MtAmazonDynamoDbContextProvider mtContext;
    private String delimiter;
    private MtTableDescriptionRepo mtTableDescriptionRepo;
    private TableMappingFactory tableMappingFactory;
    private CreateTableRequestFactory createTableRequestFactory;
    private DynamoSecondaryIndexMapper secondaryIndexMapper;
    private Boolean deleteTableAsync;
    private Boolean truncateOnDeleteTable;
    private Boolean precreateTables;
    private Integer pollIntervalSeconds;
    private Optional<String> tablePrefix = empty();

    /**
     * TODO: write Javadoc.
     *
     * @return a newly created {@code MtAmazonDynamoDbBySharedTable} based on the contents of the
     *     {@code SharedTableCustomDynamicBuilder}
     */
    public MtAmazonDynamoDbBySharedTable build() {
        setDefaults();
        validate();
        if (tableMappingFactory == null) {
            tableMappingFactory = new TableMappingFactory(
                createTableRequestFactory,
                mtContext,
                secondaryIndexMapper,
                delimiter,
                amazonDynamoDb,
                precreateTables,
                pollIntervalSeconds
            );
        }
        return new MtAmazonDynamoDbBySharedTable(name,
            mtContext,
            amazonDynamoDb,
            tableMappingFactory,
            mtTableDescriptionRepo,
            deleteTableAsync,
            truncateOnDeleteTable);
    }

    public static SharedTableCustomDynamicBuilder builder() {
        return new SharedTableCustomDynamicBuilder();
    }

    public SharedTableCustomDynamicBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public SharedTableCustomDynamicBuilder withAmazonDynamoDb(AmazonDynamoDB amazonDynamoDb) {
        this.amazonDynamoDb = amazonDynamoDb;
        return this;
    }

    public SharedTableCustomDynamicBuilder withContext(MtAmazonDynamoDbContextProvider mtContext) {
        this.mtContext = mtContext;
        return this;
    }

    public SharedTableCustomDynamicBuilder withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public SharedTableCustomDynamicBuilder withTablePrefix(String tablePrefix) {
        this.tablePrefix = of(tablePrefix);
        return this;
    }

    public SharedTableCustomDynamicBuilder withCreateTableRequestFactory(
        CreateTableRequestFactory createTableRequestFactory) {
        this.createTableRequestFactory = createTableRequestFactory;
        return this;
    }

    public SharedTableCustomDynamicBuilder withDynamoSecondaryIndexMapper(
        DynamoSecondaryIndexMapper dynamoSecondaryIndexMapper) {
        this.secondaryIndexMapper = dynamoSecondaryIndexMapper;
        return this;
    }

    public SharedTableCustomDynamicBuilder withTableDescriptionRepo(MtTableDescriptionRepo mtTableDescriptionRepo) {
        this.mtTableDescriptionRepo = mtTableDescriptionRepo;
        return this;
    }

    public SharedTableCustomDynamicBuilder withDeleteTableAsync(boolean dropAsync) {
        deleteTableAsync = dropAsync;
        return this;
    }

    public SharedTableCustomDynamicBuilder withTruncateOnDeleteTable(Boolean truncateOnDrop) {
        truncateOnDeleteTable = truncateOnDrop;
        return this;
    }

    public SharedTableCustomDynamicBuilder withPrecreateTables(boolean precreateTables) {
        this.precreateTables = precreateTables;
        return this;
    }

    public SharedTableCustomDynamicBuilder withPollIntervalSeconds(Integer pollIntervalSeconds) {
        this.pollIntervalSeconds = pollIntervalSeconds;
        return this;
    }

    protected Optional<String> getTablePrefix() {
        return tablePrefix;
    }

    private void validate() {
        checkNotNull(amazonDynamoDb, "amazonDynamoDb is required");
        checkNotNull(mtContext, "mtContext is required");
        checkNotNull(createTableRequestFactory, "createTableRequestFactory is required");
    }

    protected void setDefaults() {
        if (name == null) {
            name = "MtAmazonDynamoDbBySharedTable";
        }
        if (secondaryIndexMapper == null) {
            secondaryIndexMapper = new DynamoSecondaryIndexMapperByNameImpl();
        }
        if (delimiter == null) {
            delimiter = ".";
        }
        if (truncateOnDeleteTable == null) {
            truncateOnDeleteTable = false;
        }
        if (deleteTableAsync == null) {
            deleteTableAsync = false;
        }
        if (precreateTables == null) {
            precreateTables = true;
        }
        if (pollIntervalSeconds == null) {
            pollIntervalSeconds = 0;
        }
        if (mtTableDescriptionRepo == null) {
            mtTableDescriptionRepo = MtDynamoDbTableDescriptionRepo.builder()
                .withAmazonDynamoDb(amazonDynamoDb)
                .withContext(mtContext)
                .withTableDescriptionTableName(DEFAULT_TABLE_DESCRIPTION_TABLENAME)
                .withPollIntervalSeconds(pollIntervalSeconds)
                .withTablePrefix(tablePrefix).build();
        }
    }

    String prefix(String tableName) {
        return prefix(tablePrefix, tableName);
    }

    protected static String prefix(Optional<String> tablePrefix, String tableName) {
        return tablePrefix.map(tablePrefix1 -> tablePrefix1 + tableName).orElse(tableName);
    }

}