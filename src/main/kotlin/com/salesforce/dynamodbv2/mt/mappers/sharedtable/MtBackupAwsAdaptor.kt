package com.salesforce.dynamodbv2.mt.mappers.sharedtable

import com.amazonaws.services.dynamodbv2.model.BackupStatus
import com.amazonaws.services.dynamodbv2.model.BackupSummary
import java.util.*

/**
 * Convert simple pojos defined in {@link MtBackupManager} to and from AWS objects defined in AWS specs.
 */
class MtBackupAwsAdaptor {


    fun getBackupSummary(mtBackupMetadata: MtBackupMetadata): BackupSummary {
        return BackupSummary()
                .withBackupName(mtBackupMetadata.mtBackupId)
                .withBackupArn(mtBackupMetadata.mtBackupId)
                .withBackupCreationDateTime(Date(mtBackupMetadata.creationTime))
                .withBackupStatus(getBackupStatus(mtBackupMetadata.status))
    }

    fun getBackupStatus(status: Status): BackupStatus {
        return when (status) {
            Status.IN_PROGRESS -> BackupStatus.CREATING
            Status.FAILED -> BackupStatus.DELETED
            Status.COMPLETE -> BackupStatus.AVAILABLE
        }
    }
}