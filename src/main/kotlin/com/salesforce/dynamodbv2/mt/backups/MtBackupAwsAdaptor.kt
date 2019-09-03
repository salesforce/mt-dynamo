/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.backups

import com.amazonaws.services.dynamodbv2.model.BackupDescription
import com.amazonaws.services.dynamodbv2.model.BackupDetails
import com.amazonaws.services.dynamodbv2.model.BackupStatus
import com.amazonaws.services.dynamodbv2.model.BackupSummary
import com.amazonaws.services.dynamodbv2.model.DescribeBackupResult
import java.util.Date

/**
 * Convert simple POJOs defined in {@link MtBackupManager} to and from AWS objects defined in AWS specs.
 */
class MtBackupAwsAdaptor {

    fun getBackupSummary(backupMetadata: TenantBackupMetadata, backupArn: String): BackupSummary {
        return BackupSummary()
                .withBackupArn(backupArn)
                .withBackupName(backupMetadata.backupName)
                .withBackupCreationDateTime(Date(backupMetadata.snapshotTime))
                .withBackupStatus(getBackupStatus(backupMetadata.status))
    }

    fun getBackupSummary(mtBackupMetadata: MtBackupMetadata): BackupSummary {
        return BackupSummary()
                .withBackupName(mtBackupMetadata.mtBackupName)
                .withBackupArn(mtBackupMetadata.mtBackupName)
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

    fun getBackupDescription(mtBackupMetadata: MtBackupMetadata): BackupDescription {
        return BackupDescription()
                .withBackupDetails(getBackupDetails(mtBackupMetadata))
    }

    fun getDescribeBackupResult(mtBackupMetadata: MtBackupMetadata): DescribeBackupResult {
        return DescribeBackupResult().withBackupDescription(
                BackupDescription().withBackupDetails(getBackupDetails(mtBackupMetadata)))
    }

    private fun getBackupDetails(mtBackupMetadata: MtBackupMetadata): BackupDetails = BackupDetails()
            .withBackupName(mtBackupMetadata.mtBackupName)
            .withBackupArn(mtBackupMetadata.mtBackupName)
            .withBackupCreationDateTime(Date(mtBackupMetadata.creationTime))
            .withBackupStatus(getBackupStatus(mtBackupMetadata.status))
}

val backupAdaptorSingleton: MtBackupAwsAdaptor = MtBackupAwsAdaptor()