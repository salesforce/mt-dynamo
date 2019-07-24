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

    fun getDescribeBackupResult(mtBackupMetadata: MtBackupMetadata): DescribeBackupResult {
        return DescribeBackupResult().withBackupDescription(
                BackupDescription().withBackupDetails(BackupDetails()
                        .withBackupName(mtBackupMetadata.mtBackupId)
                        .withBackupArn(mtBackupMetadata.mtBackupId)
                        .withBackupCreationDateTime(Date(mtBackupMetadata.creationTime))
                        .withBackupStatus(getBackupStatus(mtBackupMetadata.status))))
    }
}

val backupAdaptorSingleton: MtBackupAwsAdaptor = MtBackupAwsAdaptor()