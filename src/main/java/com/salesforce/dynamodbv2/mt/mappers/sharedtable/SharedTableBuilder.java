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
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.index.HasPrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapper;
import com.salesforce.dynamodbv2.mt.mappers.index.PrimaryKeyMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * Suppresses "Line is longer than 120 characters [LineLengthCheck]" warning.  The line length violation was deemed
 * acceptable in this case for the sake of making the table more readable.
 */
@SuppressWarnings("checkstyle:LineLength")
/**
 * Maps virtual tables to a set of physical tables hard-coded into the builder by comparing the types of the elements
 * of the virtual table's primary key against the corresponding types on the physical tables.  It requires that for
 * any virtual table referenced by a client, there exists a physical table in the list predefined by the builder
 * with a primary key whose elements are compatible.  It also requires that for any secondary index on a virtual
 * table referenced by a client, there must exist a secondary index on the corresponding physical table of the same
 * type (global vs. local) where the primary keys are compatible.
 *
 * See "Table and Secondary Index Primary Key Compatibility" for an explanation of compatibility.
 *
 * The builder requires ...
 *
 * - an {@code AmazonDynamoDB} instance
 * - a multitenant context
 *
 * Optionally ...
 * - a list of {@code CreateTableRequest}s representing physical tables.  Default: See enumerated list of tables below.
 *
 * See {@code SharedTableCustomDynamicBuilder} for optional arguments and limitations.
 *
 * Below is are the physical tables that are created.  Virtual tables with no LSI will be mapped to the *_nolsi tables
 * and won't be subject to the 10GB table size limit.  Otherwise, virtual tables are mapped to their physical
 * counterpart based on the rules described in {@code PrimaryKeyMapperByTypeImpl}.
 *
 *                                  table           gsi                                                                                             lsi
 *                                  hash    range   hash        range       hash        range       hash        range       hash        range       hash    range       hash    range       hash        range
 *                                                  1                       2                       3                       4                       1       2           3
 *                                                  gsi_s_s                 gsi_s_n                 gsi_s_b                 gsi_s       -           lsi_s_s             lsi_s_n             lsi_s_ b
 *                                  hk      rk      gsi_s_s_hk  gsi_s_s_rk  gsi_s_n_hk  gsi_s_n_rk  gsi_s_b_hk  gsi_s_b_rk  gsi_s_hk    -           hk      lsi_s_s_rk  hk      lsi_s_n_rk  hk          lsi_s_b_rk
 *  mt_sharedtablestatic_s_s        S       S       S           S           S           N           S           B           S           -           S       S           S       N           S           B
 *  mt_sharedtablestatic_s_n        S       N       S           S           S           N           S           B           S           -           S       S           S       N           S           B
 *  mt_sharedtablestatic_s_b        S       B       S           S           S           N           S           B           S           -           S       S           S       N           S           B
 *  mt_sharedtablestatic_s_nolsi    S       -       S           S           S           N           S           B           S           -
 *  mt_sharedtablestatic_s_s_nolsi  S       S       S           S           S           N           S           B           S           -
 *  mt_sharedtablestatic_s_n_nolsi  S       N       S           S           S           N           S           B           S           -
 *  mt_sharedtablestatic_s_b_nolsi  S       B       S           S           S           N           S           B           S           -
 *
 * Design constraints:
 *
 * - In order to support multitenancy, all HKs (table and index-level) must be prefixed with the alphanumeric tenant ID.
 *   Therefore, all HKs must be of type S.
 * - Tables with LSIs are limited to 10GB
 *   (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html#LSI.ItemCollections.SizeLimit).
 *   Therefore, we have two sets of tables, one set with LSIs and one set without.
 * - Tables with HK only may not have LSIs (error: "AmazonServiceException: Local Secondary indices are not allowed on
 *   hash tables, only hash and range tables").  Therefore, the only table that has HK only does not have an LSI.
 * - Virtual tables with only a HK may not be mapped to a table that has both a HK and RK per
 *   https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html, "When you add an item, the primary
 *   key attribute(s) are the only required attributes.  Attribute values cannot be null."  See
 *   {@link SharedTableRangeKeyTest} for a simple test that demonstrates this.
 * - All virtual types could have been mapped into a set of tables that have only byte array types, and convert
 *   all virtual types down to byte arrays and back.  This would necessitate a smaller set of tables, possibly as few
 *   as 3.  However, the mapping layer would also need to be responsible for maintaining consistency with respect to
 *   sorting so it was not implemented.
 */
public class SharedTableBuilder extends SharedTableCustomDynamicBuilder {

    private List<CreateTableRequest> createTableRequests;
    private Long defaultProvisionedThroughput; /* TODO if this is ever going to be used in production we will need
                                                       more granularity, like at the table, index, read, write level */
    private Boolean streamsEnabled;

    public static SharedTableBuilder builder() {
        return new SharedTableBuilder();
    }

    /**
     * TODO: write Javadoc.
     *
     * @param createTableRequests the {@code CreateTableRequest}s representing the physical tables
     * @return a newly created {@code SharedTableBuilder} based on the contents of the {@code SharedTableBuilder}
     */
    public SharedTableBuilder withCreateTableRequests(CreateTableRequest... createTableRequests) {
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

    public SharedTableBuilder withDefaultProvisionedThroughput(long defaultProvisionedThroughput) {
        this.defaultProvisionedThroughput = defaultProvisionedThroughput;
        return this;
    }

    /**
     * TODO: write Javadoc.
     */
    public MtAmazonDynamoDbBySharedTable build() {
        setDefaults();
        withName("SharedTableBuilder");
        withCreateTableRequestFactory(new SharedTableCreateTableRequestFactory(createTableRequests,
            getTablePrefix()));
        withDynamoSecondaryIndexMapper(new DynamoSecondaryIndexMapperByTypeImpl());
        return super.build();
    }

    protected void setDefaults() {
        if (this.defaultProvisionedThroughput == null) {
            this.defaultProvisionedThroughput = 1L;
        }
        if (streamsEnabled == null) {
            streamsEnabled = true;
        }
        if (this.createTableRequests == null || this.createTableRequests.isEmpty()) {
            this.createTableRequests = buildDefaultCreateTableRequests(this.defaultProvisionedThroughput,
                this.streamsEnabled);
        }
        super.setDefaults();
    }

    private static final String HASH_KEY_FIELD = "hk";
    private static final String RANGE_KEY_FIELD = "rk";

    /**
     * Builds the tables underlying the SharedTable implementation as described in the class-level Javadoc.
     */
    static List<CreateTableRequest> buildDefaultCreateTableRequests(long provisionedThroughput,
        boolean streamsEnabled) {

        CreateTableRequestBuilder mtSharedTableStaticSs = CreateTableRequestBuilder.builder()
            .withTableName("mt_sharedtablestatic_s_s")
            .withTableKeySchema(HASH_KEY_FIELD, S, RANGE_KEY_FIELD, S);
        CreateTableRequestBuilder mtSharedTableStaticSn = CreateTableRequestBuilder.builder()
            .withTableName("mt_sharedtablestatic_s_n")
            .withTableKeySchema(HASH_KEY_FIELD, S, RANGE_KEY_FIELD, N);
        CreateTableRequestBuilder mtSharedTableStaticSb = CreateTableRequestBuilder.builder()
            .withTableName("mt_sharedtablestatic_s_b")
            .withTableKeySchema(HASH_KEY_FIELD, S, RANGE_KEY_FIELD, B);
        CreateTableRequestBuilder mtSharedTableStaticsNoLsi = CreateTableRequestBuilder.builder()
            .withTableName("mt_sharedtablestatic_s_nolsi")
            .withTableKeySchema(HASH_KEY_FIELD, S);
        CreateTableRequestBuilder mtSharedTableStaticSsNoLsi = CreateTableRequestBuilder.builder()
            .withTableName("mt_sharedtablestatic_s_s_nolsi")
            .withTableKeySchema(HASH_KEY_FIELD, S, RANGE_KEY_FIELD, S);
        CreateTableRequestBuilder mtSharedTableStaticSnNoLsi = CreateTableRequestBuilder.builder()
            .withTableName("mt_sharedtablestatic_s_n_nolsi")
            .withTableKeySchema(HASH_KEY_FIELD, S, RANGE_KEY_FIELD, N);
        CreateTableRequestBuilder mtSharedTableStaticSbNoLsi = CreateTableRequestBuilder.builder()
            .withTableName("mt_sharedtablestatic_s_b_nolsi")
            .withTableKeySchema(HASH_KEY_FIELD, S, RANGE_KEY_FIELD, B);

        return ImmutableList.of(mtSharedTableStaticSs,
            mtSharedTableStaticSn,
            mtSharedTableStaticSb,
            mtSharedTableStaticsNoLsi,
            mtSharedTableStaticSsNoLsi,
            mtSharedTableStaticSnNoLsi,
            mtSharedTableStaticSbNoLsi
        ).stream().map(createTableRequestBuilder -> {
            addSis(createTableRequestBuilder, provisionedThroughput);
            addStreamSpecification(createTableRequestBuilder, streamsEnabled);
            return createTableRequestBuilder.withProvisionedThroughput(provisionedThroughput,
                provisionedThroughput).build();
        }).collect(Collectors.toList());
    }

    private static void addSis(CreateTableRequestBuilder createTableRequestBuilder, long defaultProvisionedThroughput) {
        addSi(createTableRequestBuilder, GSI, S, empty(), defaultProvisionedThroughput);
        addSi(createTableRequestBuilder, GSI, S, of(S), defaultProvisionedThroughput);
        addSi(createTableRequestBuilder, GSI, S, of(N), defaultProvisionedThroughput);
        addSi(createTableRequestBuilder, GSI, S, of(B), defaultProvisionedThroughput);
        if (!createTableRequestBuilder.getTableName().toLowerCase().endsWith("nolsi")) {
            addSi(createTableRequestBuilder, LSI, S, of(S), defaultProvisionedThroughput);
            addSi(createTableRequestBuilder, LSI, S, of(N), defaultProvisionedThroughput);
            addSi(createTableRequestBuilder, LSI, S, of(B), defaultProvisionedThroughput);
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

    private static class CreateTableRequestWrapper implements HasPrimaryKey {

        private final CreateTableRequest createTableRequest;

        CreateTableRequestWrapper(CreateTableRequest createTableRequest) {
            this.createTableRequest = createTableRequest;
        }

        @Override
        public PrimaryKey getPrimaryKey() {
            return new DynamoTableDescriptionImpl(createTableRequest).getPrimaryKey();
        }

        public CreateTableRequest getCreateTableRequest() {
            return createTableRequest;
        }

    }

    /**
     * Implements the request factory that is capable of mapping virtual tables to the physical tables underlying
     * the SharedTable multitenancy strategy as described in the class-level Javadoc.
     */
    static class SharedTableCreateTableRequestFactory implements CreateTableRequestFactory {

        private final PrimaryKeyMapper primaryKeyMapper = new PrimaryKeyMapperByTypeImpl(false);
        private final List<CreateTableRequest> createTableRequests;

        /**
         * Public constructor.
         */
        SharedTableCreateTableRequestFactory(List<CreateTableRequest> createTableRequests,
                                             Optional<String> tablePrefix) {
            this.createTableRequests = createTableRequests.stream()
                .map(createTableRequest -> createTableRequest.withTableName(
                    prefix(tablePrefix, createTableRequest.getTableName())))
                .collect(Collectors.toList());
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

        private boolean isEmpty(List lsis) {
            return lsis == null || lsis.isEmpty();
        }

        @Override
        public List<CreateTableRequest> getPhysicalTables() {
            return createTableRequests;
        }

    }

}