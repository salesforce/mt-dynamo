/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
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
     * Create a multi-tenant (virtual) table with specs defined in @param createTableRequest under the given
     * multi-tenant contexts' namespace.
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
    fun listVirtualTableMetadatas(limit: Int,
            exclusiveStartTableMetadata: TenantTableMetadata?) : ListMetadataResult

    /**
     * TODO: these below methods are placeholders from implementing this interface in Java.
     * Once the impl is converted to Kotlin, all the below signatures can be removed with defaulted params.
     */
    fun listVirtualTableMetadatas(exclusiveStartTableMetadata: TenantTableMetadata?) : ListMetadataResult
    fun listVirtualTableMetadatas(limit: Int) : ListMetadataResult
    fun listVirtualTableMetadatas() : ListMetadataResult

    data class ListMetadataResult(val metadataList: List<TenantTableMetadata>, val lastEvaluatedTable: TenantTableMetadata?)

    data class TenantTableMetadata(val tenantTable: TenantTable, val createTableRequest: CreateTableRequest)

}