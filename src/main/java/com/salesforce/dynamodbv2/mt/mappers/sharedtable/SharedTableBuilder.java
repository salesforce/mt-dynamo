/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

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
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MTAmazonDynamoDBBySharedTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;
import static java.util.Optional.empty;
import static java.util.Optional.of;

/*
 * Maps virtual tables to a set of physical tables hard-coded into the builder by comparing the types of the elements
 * of the virtual table's primary key against the corresponding types on the physical tables.  It requires that for
 * any virtual table referenced by a client, there exists a physical table in the list predefined by the builder
 * with a primary key whose elements are compatible.  It also requires that for any secondary index on a virtual
 * table referenced by a client, there must exist a secondary index on the corresponding physical table of the same
 * type(global vs local) where the primary keys are compatible.
 *
 * See "Table and Secondary Index Primary Key Compatibility" for an explanation of compatibility.
 *
 * The builder requires ...
 *
 * - an AmazonDynamoDB instance
 * - a multi-tenant context
 *
 * Optionally ...
 * - a list of CreateTableRequest's representing physical tables.  Default: See enumerated list of tables below.
 *
 * See SharedTableCustomDynamicBuilder for optional arguments and limitations.
 *
 * Below is are the physical tables that are created.  Virtual tables with no LSI will be mapped to the *_nolsi tables
 * and won't be subject to the 10GB table size limit.  Otherwise, virtual tables are mapped to their physical counterpart
 * based on the rules described in PrimaryKeyMapperByTypeImpl.
 *
 * 	                                table			gsi									                                                            lsi
 * 	                                hash	range	hash	    range	    hash	    range	    hash	    range	    hash	    range		hash	range	    hash	range	    hash	    range
 * 	                                                1		                2		                3		                4			            1		2		    3
 * 	                                                gsi_s_s		            gsi_s_n		            gsi_s_b		            gsi_s	    -		    lsi_s_s	            lsi_s_n	            lsi_s_ b
 * 	                                hk	    rk		gsi_s_s_hk	gsi_s_s_rk	gsi_s_n_hk	gsi_s_n_rk	gsi_s_b_hk	gsi_s_b_rk	gsi_s_hk	-		    hk	    lsi_s_s_rk	hk	    lsi_s_n_rk	hk	        lsi_s_b_rk
 * 	mt_sharedtablestatic_s_s	    S	    S		S	        S	        S	        N	        S	        B	        S	        -		    S	    S	        S	    N	        S	        B
 * 	mt_sharedtablestatic_s_n	    S	    N		S	        S	        S	        N	        S	        B	        S	        -		    S	    S	        S	    N	        S	        B
 * 	mt_sharedtablestatic_s_b	    S	    B		S	        S	        S	        N	        S	        B	        S	        -		    S	    S	        S	    N	        S	        B
 * 	mt_sharedtablestatic_s_nolsi	S	    -		S	        S	        S	        N	        S	        B	        S	        -
 * 	mt_sharedtablestatic_s_s_nolsi	S	    S		S	        S	        S	        N	        S	        B	        S	        -
 * 	mt_sharedtablestatic_s_n_nolsi	S	    N		S	        S	        S	        N	        S	        B	        S	        -
 * 	mt_sharedtablestatic_s_b_nolsi	S	    B		S	        S	        S	        N	        S	        B	        S	        -
 *
 * Design constraints:
 *
 * - In order to support multi-tenancy, all HK's(table and index-level) must be prefixed with the alphanumeric tenant id.
 *   Therefore, all HK's must be of type S.
 * - Tables with LSIs are limited to 10GB(https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html#LSI.ItemCollections.SizeLimit).
 *   Therefore, we have two sets of tables, one set with LSIs and one set without.
 * - Tables with HK only may not have LSIs (error: "AmazonServiceException: Local Secondary indices are not allowed on
 *   hash tables, only hash and range tables").  Therefore, the only table that has HK only does not have an LSI.
 * - Virtual tables with only a HK may not be mapped to a table that has both a HK and RK per
 *   https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html, "When you add an item, the primary
 *   key attribute(s) are the only required attributes."
 * - All virtual types could have been mapped into a set of tables that have only byte array types, and convert
 *   all virtual types down to byte arrays and back.  This would necessitate a smaller set of tables, possibly as few
 *   as 3.  However, the mapping layer would also need to be responsible for maintaining consistency with respect to
 *   sorting so it was not implemented.
 */
public class SharedTableBuilder extends SharedTableCustomDynamicBuilder {

    private List<CreateTableRequest> createTableRequests;
    private Boolean precreateTables;
    private Long defaultProvisionedThroughput; /* TODO if this is ever going to be used in production we will need
                                                       more granularity, like at the table, index, read, write level */

    @SuppressWarnings("WeakerAccess")
    public static SharedTableBuilder builder() {
        return new SharedTableBuilder();
    }

    @SuppressWarnings("unused")
    public SharedTableBuilder withCreateTableRequests(CreateTableRequest... createTableRequests) {
        if (this.createTableRequests == null) {
            this.createTableRequests = new ArrayList<>();
        }
        this.createTableRequests.addAll(Arrays.asList(createTableRequests)); return this;
    }

    public SharedTableBuilder withPrecreateTables(boolean precreateTables) {
        this.precreateTables = precreateTables; return this;
    }

    @SuppressWarnings("unused")
    public SharedTableBuilder withDefaultProvisionedThroughput(long defaultProvisionedThroughput) {
        this.defaultProvisionedThroughput = defaultProvisionedThroughput; return this;
    }

    public MTAmazonDynamoDBBySharedTable build() {
        setDefaults();
        withName("SharedTableBuilder");
        withCreateTableRequestFactory(new SharedTableCreateTableRequestFactory(createTableRequests, precreateTables));
        withDynamoSecondaryIndexMapper(new DynamoSecondaryIndexMapperByTypeImpl());
        return super.build();
    }

    protected void setDefaults() {
        if (this.defaultProvisionedThroughput == null) {
            this.defaultProvisionedThroughput = 1L;
        }
        if (this.createTableRequests == null || this.createTableRequests.isEmpty()) {
            this.createTableRequests = buildDefaultCreateTableRequests(this.defaultProvisionedThroughput);
        }
        if (precreateTables == null) {
            precreateTables = true;
        }
        super.setDefaults();
    }

    private static final String hashKeyField = "hk";
    private static final String rangeKeyField = "rk";

    private List<CreateTableRequest> buildDefaultCreateTableRequests(long provisionedThroughput) {

        CreateTableRequestBuilder mt_sharedtablestatic_s_s = CreateTableRequestBuilder.builder()
                .withTableName("mt_sharedtablestatic_s_s")
                .withTableKeySchema(hashKeyField, S, rangeKeyField, S);
        CreateTableRequestBuilder mt_sharedtablestatic_s_n = CreateTableRequestBuilder.builder()
                .withTableName("mt_sharedtablestatic_s_n")
                .withTableKeySchema(hashKeyField, S, rangeKeyField, N);
        CreateTableRequestBuilder mt_sharedtablestatic_s_b = CreateTableRequestBuilder.builder()
                .withTableName("mt_sharedtablestatic_s_b")
                .withTableKeySchema(hashKeyField, S, rangeKeyField, B);
        CreateTableRequestBuilder mt_sharedtablestatic_s_nolsi = CreateTableRequestBuilder.builder()
                .withTableName("mt_sharedtablestatic_s_nolsi")
                .withTableKeySchema(hashKeyField, S);
        CreateTableRequestBuilder mt_sharedtablestatic_s_s_nolsi = CreateTableRequestBuilder.builder()
                .withTableName("mt_sharedtablestatic_s_s_nolsi")
                .withTableKeySchema(hashKeyField, S, rangeKeyField, S);
        CreateTableRequestBuilder mt_sharedtablestatic_s_n_nolsi = CreateTableRequestBuilder.builder()
                .withTableName("mt_sharedtablestatic_s_n_nolsi")
                .withTableKeySchema(hashKeyField, S, rangeKeyField, N);
        CreateTableRequestBuilder mt_sharedtablestatic_s_b_nolsi = CreateTableRequestBuilder.builder()
                .withTableName("mt_sharedtablestatic_s_b_nolsi")
                .withTableKeySchema(hashKeyField, S, rangeKeyField, B);

        return ImmutableList.of(mt_sharedtablestatic_s_s,
                                mt_sharedtablestatic_s_n,
                                mt_sharedtablestatic_s_b,
                                mt_sharedtablestatic_s_nolsi,
                                mt_sharedtablestatic_s_s_nolsi,
                                mt_sharedtablestatic_s_n_nolsi,
                                mt_sharedtablestatic_s_b_nolsi
                                ).stream().map(createTableRequestBuilder -> {
            addSIs(createTableRequestBuilder);
            addStreamSpecification(createTableRequestBuilder);
            return createTableRequestBuilder.withProvisionedThroughput(provisionedThroughput,
                    provisionedThroughput).build();
        }).collect(Collectors.toList());
    }

    private void addSIs(CreateTableRequestBuilder createTableRequestBuilder) {
        addSI(createTableRequestBuilder, GSI, S, empty());
        addSI(createTableRequestBuilder, GSI, S, of(S));
        addSI(createTableRequestBuilder, GSI, S, of(N));
        addSI(createTableRequestBuilder, GSI, S, of(B));
        if (!createTableRequestBuilder.getTableName().toLowerCase().endsWith( "nolsi")) {
            addSI(createTableRequestBuilder, LSI, S, of(S));
            addSI(createTableRequestBuilder, LSI, S, of(N));
            addSI(createTableRequestBuilder, LSI, S, of(B));
        }
    }

    private void addStreamSpecification(CreateTableRequestBuilder createTableRequestBuilder) {
        createTableRequestBuilder.withStreamSpecification(new StreamSpecification()
                        .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                        .withStreamEnabled(true));
    }

    private void addSI(CreateTableRequestBuilder createTableRequestBuilder,
                       DynamoSecondaryIndexType indexType,
                       @SuppressWarnings("all")
                               ScalarAttributeType hashKeyType,
                       Optional<ScalarAttributeType> rangeKeyType) {
        String indexName = indexType.name().toLowerCase() + "_" +
            hashKeyType.name().toLowerCase() +
            rangeKeyType.map(type -> "_" + type.name().toLowerCase()).orElse("").toLowerCase();
        PrimaryKey primaryKey = rangeKeyType.map(
                scalarAttributeType -> new PrimaryKey(indexType == LSI ? "hk" : indexName + "_hk",
                                                      hashKeyType,
                                                      indexName + "_rk",
                                                      scalarAttributeType))
                .orElseGet(() -> new PrimaryKey(indexName + "_hk",
                                                hashKeyType));
        createTableRequestBuilder.addSI(indexName,
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

    private class SharedTableCreateTableRequestFactory implements CreateTableRequestFactory {

        private final PrimaryKeyMapper primaryKeyMapper = new PrimaryKeyMapperByTypeImpl(false);
        private final List<CreateTableRequest> createTableRequests;
        private final boolean precreateTables;

        SharedTableCreateTableRequestFactory(List<CreateTableRequest> createTableRequests,
                                             boolean precreateTables) {
            this.createTableRequests = createTableRequests.stream()
                    .map(createTableRequest -> createTableRequest.withTableName(prefix(createTableRequest.getTableName())))
                    .collect(Collectors.toList());
            this.precreateTables = precreateTables;
        }

        @Override
        public CreateTableRequest getCreateTableRequest(DynamoTableDescription virtualTableDescription) {
            try {
                boolean hasLSIs = !isEmpty(virtualTableDescription.getLSIs());
                return ((CreateTableRequestWrapper) primaryKeyMapper
                        .mapPrimaryKey(virtualTableDescription.getPrimaryKey(), createTableRequests.stream()
                                .filter(createTableRequest1 -> hasLSIs == !isEmpty(createTableRequest1.getLocalSecondaryIndexes()))
                                .map((Function<CreateTableRequest, HasPrimaryKey>) CreateTableRequestWrapper::new).collect(Collectors.toList())))
                        .getCreateTableRequest();
            } catch (MappingException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("all")
        private boolean isEmpty(List lsis) {
            return lsis == null || lsis.isEmpty();
        }

        @Override
        public List<CreateTableRequest> precreateTables() {
            return precreateTables ? createTableRequests : new ArrayList<>();
        }

    }

}