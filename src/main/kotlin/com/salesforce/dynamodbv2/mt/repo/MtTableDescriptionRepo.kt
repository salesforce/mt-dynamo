/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.repo

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest
import com.amazonaws.services.dynamodbv2.model.ListTablesResult
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb

/**
 * Interface for managing table metadata of virtual tables within mt-dynamo.
 *
 * @author msgroi
 */
interface MtTableDescriptionRepo {

    /**
     * Create a virtual table's metadata record with specs defined in param createTableRequest under the given
     * multitenant context's namespace.
     *
     * @param createTableRequest specs of the table to create
     * @param isMultitenant whether this is an explicitly multitenant table
     * @return the table's description
     */
    fun createTableMetadata(createTableRequest: CreateTableRequest, isMultitenant: Boolean): MtTableDescription

    /**
     * @param tableName name of the table to look up
     * @return the table description of the given virtual table under the current multitenant context
     */
    fun getTableDescription(tableName: String): MtTableDescription

    /**
     * Delete the designated virtual table metadata for a given tenant context.
     *
     * @param tableName name of the table to delete
     */
    fun deleteTableMetadata(tableName: String)

    fun getTableDescriptionTableName(): String

    /**
     * Utility to enumerate all virtual table metadata managed by this instance. Return up to {@code limit} results,
     * starting after {@code exclusiveStartTableMetadata} if specified.
     *
     * @return a list of {@code TenantTableMetadata} objects, with {@code lastEvaluatedTable} populated with the last
     * table name if the result set is fully populated with {@code limit} results, otherwise null.
     */
    fun listVirtualTableMetadata(listMetadataRequest: ListMetadataRequest): ListMetadataResult

    /**
     * Enumerate all virtual table names associated with the given tenant context.
     */
    fun listTables(listTablesRequest: ListTablesRequest): ListTablesResult

    data class ListMetadataResult(
        val createTableRequests: List<MtCreateTableRequest>,
        val lastEvaluatedTable: MtCreateTableRequest?
    )

    data class ListMetadataRequest(var limit: Int = DEFAULT_SCAN_LIMIT, var startTenantTableKey: MtAmazonDynamoDb.TenantTable? = null) {

        fun withLimit(limit: Int?): ListMetadataRequest {
            if (limit == null) {
                this.limit = DEFAULT_SCAN_LIMIT
            } else {
                this.limit = limit
            }
            return this
        }

        fun withExclusiveStartCreateTableReq(startKey: MtCreateTableRequest?): ListMetadataRequest {
            startTenantTableKey = if (startKey == null) {
                null
            } else {
                MtAmazonDynamoDb.TenantTable(startKey.createTableRequest.tableName, startKey.tenantName)
            }
            return this
        }

        fun withExclusiveStartKey(startTenantTable: MtAmazonDynamoDb.TenantTable?): ListMetadataRequest {
            startTenantTableKey = startTenantTable
            return this
        }
    }

    data class MtCreateTableRequest(
        val tenantName: String,
        val createTableRequest: CreateTableRequest
    )
}

private const val DEFAULT_SCAN_LIMIT = 100
