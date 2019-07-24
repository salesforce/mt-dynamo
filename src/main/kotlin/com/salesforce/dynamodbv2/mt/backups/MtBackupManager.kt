/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.backups
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import com.amazonaws.services.dynamodbv2.model.BackupSummary
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest
import com.amazonaws.services.dynamodbv2.model.ListBackupsRequest
import com.amazonaws.services.dynamodbv2.model.ListBackupsResult
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupRequest
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbBase

/**
 * Interface for grabbing backups of data managed by mt-dynamo.
 *
 * Backups are generated across all managed tables to sliced up, granular tenant-table backups, which may independently
 * be restored. Tenant-table backups must be restored to a different tenant-table target. Additionally, a tenant-table
 * backup may be restored  onto either the same environment, or migrated into different environments, with say,
 * different multitenant strategies (ie: moving a tenant-table from a table per tenant setup
 * onto a shared table setup, or vice versa.
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
    fun createMtBackup(
        createMtBackupRequest: CreateMtBackupRequest
    ): MtBackupMetadata

    /**
     * Go through each physical row in {@code physicalTableName} in dynamo, and augment the current in progress backup
     * data on S3 with said data.
     */
    fun backupPhysicalMtTable(
        createMtBackupRequest: CreateMtBackupRequest,
        physicalTableName: String
    ): MtBackupMetadata

    /**
     * Internal function to mark an in progress backup to completed state.
     */
    fun markBackupComplete(createMtBackupRequest: CreateMtBackupRequest): MtBackupMetadata

    /**
     * Get the status of a given multi-tenant backup.
     */
    fun getBackup(id: String): MtBackupMetadata?

    /**
     * Delete the give multi-tenant backup.
     */
    fun deleteBackup(id: String): MtBackupMetadata?

    /**
     * Get details of a given table-tenant backup.
     */
    fun getTenantTableBackup(id: String): TenantTableBackupMetadata

    /**
     * Initiate a restore of a given table-tenant backup to a new table-tenant target.
     */
    fun restoreTenantTableBackup(
        restoreMtBackupRequest: RestoreMtBackupRequest,
        mtContext: MtAmazonDynamoDbContextProvider
    ): TenantRestoreMetadata

    /**
     * List all multi-tenant backups known to us on S3.
     */
    fun listMtBackups(listMtBackupRequest: ListMtBackupRequest): ListMtBackupsResult
}

/**
 * Metadata of a multitenant backup.
 *
 * @param mtBackupId id of a backup configured by client.
 * @param status {@link Status} of backup
 * @param tenantTables tenant-tables contained within this given backup
 * @param creationTime timestamp this backup began processing in milliseconds since epoch
 */
data class MtBackupMetadata(
    val mtBackupId: String,
    val status: Status,
    val tenantTables: Set<TenantTableBackupMetadata>,
    val creationTime: Long = -1
) {
    /**
     * @return a new MtBackupMetadata object merging this backup metadata with {@code otherBackupMetadata}.
     */
    fun merge(newBackupMetadata: MtBackupMetadata): MtBackupMetadata {
        if (!(newBackupMetadata.mtBackupId.equals(mtBackupId))) {
            throw MtBackupException("Trying to merge a backup with a different backup id, " +
                    "this: $mtBackupId, other: ${newBackupMetadata.mtBackupId}")
        }
        return MtBackupMetadata(mtBackupId,
                newBackupMetadata.status, // use status of new metadata
                newBackupMetadata.tenantTables.plus(tenantTables),
                creationTime) // maintain existing create time for all merges
    }
}

class ListMtBackupRequest(backupLimit: Int = 5, val exclusiveStartBackup: BackupSummary? = null) : ListBackupsRequest() {
    init {
        this.limit = backupLimit
    }
}

class ListMtBackupsResult(backups: List<BackupSummary>, val lastEvaluatedBackup: BackupSummary?) : ListBackupsResult() {
    init {
        setBackupSummaries(backups)
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

data class CreateMtBackupRequest(val backupId: String, val shouldSnapshotTables: Boolean) : CreateBackupRequest() {
    init {
        backupName = backupId
    }
}

class RestoreMtBackupRequest(
    val backupId: String,
    val tenantTableBackup: MtAmazonDynamoDb.TenantTable,
    val newTenantTable: MtAmazonDynamoDb.TenantTable
) : RestoreTableFromBackupRequest() {
    init {
        backupArn = backupId
        targetTableName = newTenantTable.virtualTableName
    }
}
class MtBackupException(message: String) : AmazonDynamoDBException(message)

enum class Status {
    IN_PROGRESS,
    COMPLETE,
    FAILED
}

enum class StatusDetail {
    METADATA_SNAPSHOT,
    TABLE_SNAPSHOTTING,
    SNAPSHOT_SCANNING,
    COMPLETE,
    FAILED
}