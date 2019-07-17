/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.mappers.sharedtable
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest
import com.amazonaws.services.dynamodbv2.model.DescribeBackupRequest
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbBase
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable

/**
 * Interface for grabbing backups of data managed by mt-dynamo.
 *
 * Backups are generated across all managed tables to individual tenant-table backups, which can independently
 * be restored onto either the same environment, or migrated into different environments, with say, different
 * multi-tenant strategies (ie: moving a tenant-table from a table per tenant setup onto a shared table setup, or
 * vice versa.
 *
 * One more dimension of value added by these backups are, they should be mt-dynamo version agnostic. So if a backup
 * was generated at v0.10.5 of mt-dynamo, and imagine the physical representation of tenant to table mapping strategy
 * changes at v0.11.0, that backup should be restorable at that later version, offering a path to preserve data when
 * we change table mappings as mt-dynamo evolves.
 *
 * At the moment, these backups are taking full snapshots of an mt-dynamo account, but there are plans to support PITR
 * style continuous backups, offering a time window of available restore points (versus choosing from N snapshots)
 */
interface MtBackupManager {
    /**
     * @return a new multitenant backup job, with status {@link Status.IN_PROGRESS} if none exists with
     * specified backupId.
     */
    fun createMtBackup(createMtBackupRequest: CreateMtBackupRequest,
                       mtDynamo: MtAmazonDynamoDbBySharedTable): MtBackupMetadata

    /**
     * Go through each physical row in {@code physicalTableName} in dynamo, and augment the current in progress backup
     * data on S3 with said data.
     */
    fun backupPhysicalMtTable(createMtBackupRequest: CreateMtBackupRequest,
                              physicalTableName: String,
                              mtDynamo: MtAmazonDynamoDbBySharedTable): MtBackupMetadata

    fun markBackupComplete(createMtBackupRequest: CreateMtBackupRequest): MtBackupMetadata

    /**
     * Get the status of a given multi-tenant backup.
     */
    fun getBackup(id: String): MtBackupMetadata?

    /**
     * Delete the give multi-tenant backup.
     */
    fun deleteBackup(id: String): MtBackupMetadata?

    fun fromDynamoCreateBackupRequest(createBackupRequest: CreateBackupRequest) : CreateMtBackupRequest =
            CreateMtBackupRequest(createBackupRequest.backupName)


    fun fromDescribeBackupRequest(describeBackupRequest: DescribeBackupRequest) : MtBackupMetadata


    /**
     * Get details of a given table-tenant backup.
     */
    fun getTenantTableBackup(id: String): TenantTableBackupMetadata

    /**
     * Initiate a restore of a given table-tenant backup to a new table-tenant target.
     */
    fun restoreTenantTableBackup(
            restoreMtBackupRequest: RestoreMtBackupRequest,
            mtDynamo: MtAmazonDynamoDbBase,
            mtContext: MtAmazonDynamoDbContextProvider
    ): TenantRestoreMetadata

    /**
     * List all multi-tenant backups known to us on S3.
     */
    fun listMtBackups(): List<MtBackupMetadata>
}

class MtBackupMetadata(val mtBackupId: String,
                       val status: Status,
                       val tenantTables: Set<TenantTableBackupMetadata>,
                       val creationTime: Long = -1) {

    /**
     * @return a new MtBackupMetadata object merging this backup metadata with {@code otherBackupMetadata}
     */
    fun merge(newBackupMetadata: MtBackupMetadata): MtBackupMetadata {
        if (!newBackupMetadata.mtBackupId.equals(mtBackupId)) {
            throw MtBackupException("Trying to merge a backup with a different backup id, " +
                    "this: ${mtBackupId}, other: ${newBackupMetadata.mtBackupId}")
        }
        return MtBackupMetadata(mtBackupId,
                newBackupMetadata.status, //use status of new metadata
                newBackupMetadata.tenantTables.plus(tenantTables),
                creationTime) // maintain existing create time for all merges
    }
}

data class TenantTableBackupMetadata(
        val backupId: String,
        val status: Status,
        val tenantId: String,
        val virtualTableName: String,
        val backupKeys: Set<String>
)

data class TenantRestoreMetadata(val backupId: String, val status: Status, val tenantId: String, val virtualTableName: String)

data class CreateMtBackupRequest(val backupId: String)
data class RestoreMtBackupRequest(
    val backupId: String,
    val tenantTableBackup: MtAmazonDynamoDb.TenantTable,
    val newTenantTable: MtAmazonDynamoDb.TenantTable
)

class MtBackupException(message: String): AmazonDynamoDBException(message)

enum class Status {
    IN_PROGRESS,
    COMPLETE,
    FAILED
}