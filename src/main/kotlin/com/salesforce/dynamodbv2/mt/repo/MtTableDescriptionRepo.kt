/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.repo

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb

/**
 * Interface for managing table metadata of multitenant virtual tables within mt-dynamo.
 *
 * @author msgroi
 */
interface MtTableDescriptionRepo {

    /**
     * Create a multitenant (virtual) table with specs defined in param createTableRequest under the given multitenant
     * context's namespace.
     *
     * @param createTableRequest specs of the table to create
     * @return the table's description
     */
    fun createTable(createTableRequest: CreateTableRequest): TableDescription

    /**
     * @param tableName name of the table to look up
     * @return the table description of the given virtual table under the current multitenant context
     */
    fun getTableDescription(tableName: String): TableDescription

    /**
     * Delete the designated virtual table metadata.
     *
     * @param tableName name of the table to delete
     * @return the table description of the virtual table to be deleted
     */
    fun deleteTable(tableName: String): TableDescription

    /**
     * Utility to enumerate all virtual table metadata managed by this instance. Return up to {@code limit} results,
     * starting after {@code exclusiveStartTableMetadata} if specified.
     *
     * @return a list of {@code TenantTableMetadata} objects, with {@code lastEvaluatedTable} populated with the last
     * table name if the result set is fully populated with {@code limit} results, otherwise null.
     */
    fun listVirtualTableMetadata(listMetadataRequest: ListMetadataRequest): ListMetadataResult

    data class ListMetadataResult(
        val createTableRequests: List<MtCreateTableRequest>,
        val lastEvaluatedTable: MtCreateTableRequest?
    )

    data class ListMetadataRequest(var limit: Int = 10, var exclusiveStartTableMetadata: MtCreateTableRequest? = null, var exclusiveStartTenantTable: MtAmazonDynamoDb.TenantTable? = null) {
        fun withLimit(limit: Int): ListMetadataRequest {
            this.limit = limit
            return this
        }

        fun withExclusiveStartKey(startKey: MtCreateTableRequest?): ListMetadataRequest {
            exclusiveStartTableMetadata = startKey
            return this
        }

        fun withExclusiveStartTenantTable(startTenantTable: MtAmazonDynamoDb.TenantTable?): ListMetadataRequest {
            exclusiveStartTenantTable = startTenantTable
            return this
        }
    }

    data class MtCreateTableRequest(
        val tenantName: String,
        val createTableRequest: CreateTableRequest
    )
}
