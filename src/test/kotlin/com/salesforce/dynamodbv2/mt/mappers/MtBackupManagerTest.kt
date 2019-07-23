/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.mappers

import com.amazonaws.services.dynamodbv2.model.BackupStatus
import com.google.common.collect.ImmutableSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*

internal class MtBackupManagerTest {

    @Test
    fun testDescribeBackup() {
        val backupId = "backup-id-1234"
        val tenant = "tenant-1234"
        val table = "table-1234"
        val key = "fake-backup-file-key"
        val fakeDate = 10203040L
        val backupMetadata = MtBackupMetadata(backupId,
                Status.IN_PROGRESS,
                ImmutableSet.of(TenantTableBackupMetadata(backupId, Status.IN_PROGRESS, tenant, table, ImmutableSet.of(key))),
                fakeDate)
        val describeBackupResult = MtBackupAwsAdaptor().getDescribeBackupResult(backupMetadata)
        assertEquals(BackupStatus.CREATING.name, describeBackupResult.backupDescription.backupDetails.backupStatus)
        assertEquals(backupId, describeBackupResult.backupDescription.backupDetails.backupArn)
        assertEquals(backupId, describeBackupResult.backupDescription.backupDetails.backupName)
        assertEquals(Date(fakeDate), describeBackupResult.backupDescription.backupDetails.backupCreationDateTime)
    }
}