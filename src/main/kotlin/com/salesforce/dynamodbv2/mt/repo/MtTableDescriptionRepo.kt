/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.repo

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.TenantTable

/**
 * Interface for managing table metadata of multi-tenant virtual tables within mt-dynamo.
 *
 * @author msgroi
 */
interface MtTableDescriptionRepo {

    /**
     * Create a multitenant (virtual) table with specs defined in param createTableRequest under the given
     * multitenant context's namespace.
     *
     * @param createTableRequest specs of table to create
     * @return the table description of said table
     */
    fun createTable(createTableRequest: CreateTableRequest): TableDescription

    /**
     * @param tableName to look up
     * @return the table description of the given virtual table under the current multi-tenant context
     */
    fun getTableDescription(tableName: String): TableDescription

    /**
     * Delete the designated virtual table metadata.
     * @param tableName tpo de.lete
     * @return the table description of the virtual table deleted
     */
    fun deleteTable(tableName: String): TableDescription

    /**
     * Utility to enumerate all virtual table metadata managed by this instance. Return up to @param limit results,
     * starting after @param exclusiveStartTableMetadata if specified.
     *
     * @return a list of TenantTableMetadata objects, with a lastEvaluatedTable populated to the last table name if the
     * result set fully populated @param limit results, null otherwise.
     */
    fun listVirtualTableMetadata(
            listMetadataRequest: ListMetadataRequest
    ): ListMetadataResult

    data class ListMetadataResult(val metadataList: List<TenantTableMetadata>, val lastEvaluatedTable: TenantTableMetadata?)
    data class ListMetadataRequest(var limit: Int = 10, var exclusiveStartTableMetadata: TenantTableMetadata? = null) {
        fun withLimit(limit: Int): ListMetadataRequest {
            this.limit = limit
            return this
        }

        fun withExclusiveStartKey(startKey: TenantTableMetadata?): ListMetadataRequest {
            exclusiveStartTableMetadata = startKey
            return this
        }
    }
    data class TenantTableMetadata(val tenantTable: TenantTable, val createTableRequest: CreateTableRequest)
}

const val DEFAULT_RESULT_LIMIT = 10
@JvmField val DEFAULT_START_KEY: MtTableDescriptionRepo.TenantTableMetadata? = null
