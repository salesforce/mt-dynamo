/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import static java.util.function.Function.identity;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndexMapperByTypeImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Maps virtual tables to a set of physical tables by comparing the types of the elements of the virtual table'
 * s primary key against the corresponding types on the physical tables.  The list of physical tables is provided
 * to the builder when it is constructed along with a TableMapping implementation.  When build() is called on the
 * builder, the physical tables are created.  When requests are received for operations on a given table, the
 * TableMapper is called, passing in a virtual table description.  The TableMapper implementation returns the
 * name of the corresponding physical table.  The TableMapper may return the name of any physical table as long as
 * the table's virtual and physical primary key types are compatible.  Also for any secondary
 * index on a virtual table referenced by a client, there must exist a secondary index on the corresponding physical
 * table of the same type(global vs local) where the primary keys are compatible.
 *
 * <p>See "Table and Secondary Index Primary Key Compatibility" for an explanation of compatibility.
 *
 * <p>It requires ...
 *
 * <p>- an AmazonDynamoDB instance
 * - a multi-tenant context
 * - array of CreateTableRequest's: CreateTableRequest's representing the physical tables to be created when
 *   build() is called.
 * - a TableMapper implementation: implementation of the TableMapper interface that takes a DynamoTableDescription
 *   representing the virtual table being referenced by the API client and returns the name of a physical table.
 *   The table name returned must match one of the names of the tables passed in in the array of CreateTableRequest's.
 *
 * <p>See SharedTableCustomDynamicBuilder for optional arguments and limitations.
 */
public class SharedTableCustomStaticBuilder extends SharedTableCustomDynamicBuilder {

    private Map<String, CreateTableRequest> createTableRequestsMap;
    private TableMapper tableMapper;

    public static SharedTableCustomStaticBuilder builder() {
        return new SharedTableCustomStaticBuilder();
    }

    /**
     * TODO: write Javadoc.
     *
     * @param createTableRequests the {@code CreateTableRequest}s representing the physical tables
     * @return this {@code SharedTableCustomStaticBuilder} object
     */
    public SharedTableCustomStaticBuilder withCreateTableRequests(CreateTableRequest... createTableRequests) {
        this.createTableRequestsMap = Arrays.stream(createTableRequests)
            .collect(Collectors.toMap(CreateTableRequest::getTableName, identity()));
        return this;
    }

    public SharedTableCustomStaticBuilder withTableMapper(TableMapper tableMapper) {
        this.tableMapper = tableMapper;
        return this;
    }

    /**
     * TODO: write Javadoc.
     */
    public MtAmazonDynamoDbBySharedTable build() {
        withName("SharedTableCustomStaticBuilder");
        createTableRequestsMap.values().forEach(
            createTableRequest -> createTableRequest.withTableName(prefix(createTableRequest.getTableName())));
        withCreateTableRequestFactory(new CreateTableRequestFactory() {
            @Override
            public Optional<CreateTableRequest> getCreateTableRequest(DynamoTableDescription virtualTableDescription) {
                return Optional.of(createTableRequestsMap.get(tableMapper.mapToPhysicalTable(virtualTableDescription)));
            }

            @Override
            public List<CreateTableRequest> getPhysicalTables() {
                return new ArrayList<>(createTableRequestsMap.values());
            }
        });
        withDynamoSecondaryIndexMapper(new DynamoSecondaryIndexMapperByTypeImpl());
        return super.build();
    }

    public interface TableMapper {
        String mapToPhysicalTable(DynamoTableDescription virtualTableDescription);
    }

}