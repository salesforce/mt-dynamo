package com.salesforce.dynamodbv2.mt.sharedtable.impl

import com.amazonaws.auth.AWSCredentialsProvider

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
    fun createMtBackup(): MtBackupMetadata

    fun getBackup(id: String): MtBackupMetadata

    fun terminateBackup(id: String): MtBackupMetadata

    fun getTenantTableBackup(id: String): TenantTableBackupMetadata

    fun restoreTenantTableBackup(id: String, newTenantId: String, newTenantTableName: String): TenantRestoreMetadata

    fun listMtBackups(): List<MtBackupMetadata>
}


class MtBackupManagerImpl(awsCreds: AWSCredentialsProvider, region: String, s3BucketName: String) : MtBackupManager {
    override fun getTenantTableBackup(id: String): TenantTableBackupMetadata {
        TODO("not implemented")
    }

    override fun restoreTenantTableBackup(id: String, newTenantId: String, newTenantTableName: String): TenantRestoreMetadata {
        TODO("not implemented")
    }

    override fun listMtBackups(): List<MtBackupMetadata> {
        TODO("not implemented")
    }

    override fun getBackup(id: String): MtBackupMetadata {
        TODO("not implemented")
    }

    override fun terminateBackup(id: String): MtBackupMetadata {
        TODO("not implemented")
    }

    override fun createMtBackup(): MtBackupMetadata {
        TODO("not implemented")
    }


}

data class MtBackupMetadata(val mtBackupId: String, val status: Status, val tenantTables: Set<TenantTableBackupMetadata>)

data class TenantTableBackupMetadata(val backupId: String, val status: Status, val tenantId: String, val virtualTableName: String)

data class TenantRestoreMetadata(val backupId: String, val status: Status, val tenantId: String, val virtualTableName: String)

enum class Status {
    IN_PROGRESS,
    COMPLETE,
    FAILED
}