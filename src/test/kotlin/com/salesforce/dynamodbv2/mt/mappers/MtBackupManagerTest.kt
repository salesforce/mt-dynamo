/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.mappers

import com.amazonaws.services.dynamodbv2.model.BackupStatus
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest
import com.amazonaws.services.dynamodbv2.model.ListBackupsRequest
import com.amazonaws.services.s3.AmazonS3
import com.google.common.collect.ImmutableMap
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal
import com.salesforce.dynamodbv2.mt.backups.MtBackupAwsAdaptor
import com.salesforce.dynamodbv2.mt.backups.MtBackupMetadata
import com.salesforce.dynamodbv2.mt.backups.Status
import com.salesforce.dynamodbv2.mt.backups.TenantTableBackupMetadata
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtSharedTableBackupManagerS3It
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import java.util.Date

internal class MtBackupManagerTest {

    @Test
    fun testDescribeBackup() {
        val backupId = "backup-id-1234"
        val tenant = "tenant-1234"
        val table = "table-1234"
        val fakeDate = 10203040L
        val backupMetadata = MtBackupMetadata(backupId,
                Status.IN_PROGRESS,
                ImmutableMap.of(TenantTableBackupMetadata(backupId, tenant, table), 1L),
                fakeDate)
        val describeBackupResult = MtBackupAwsAdaptor().getDescribeBackupResult(backupMetadata)
        assertEquals(BackupStatus.CREATING.name, describeBackupResult.backupDescription.backupDetails.backupStatus)
        assertEquals(backupId, describeBackupResult.backupDescription.backupDetails.backupArn)
        assertEquals(backupId, describeBackupResult.backupDescription.backupDetails.backupName)
        assertEquals(Date(fakeDate), describeBackupResult.backupDescription.backupDetails.backupCreationDateTime)
    }

    /**
     * Given create-backup commands, use the same POJO as AWS on demand backups. There are some params relevant to
     * on-demand backups which are ignored with mt-dynamo's backup API, and thus, should throw an error if using ignored
     * params.
     */
    @Test
    fun testCreateBackupRequestInputValidation() {
        val s3Client: AmazonS3 = mock(AmazonS3::class.java)
        val dynamo = AmazonDynamoDbLocal.getAmazonDynamoDbLocal()
        val mtContext = MtAmazonDynamoDbContextProviderThreadLocalImpl()
        val sharedTableBinaryHashKey = SharedTableBuilder.builder()
                .withAmazonDynamoDb(dynamo)
                .withContext(mtContext)
                .withBackupSupport(s3Client, "fake-bucket", MtSharedTableBackupManagerS3It.MtScanningSnapshotter())
                .withTruncateOnDeleteTable(true)
                .withBinaryHashKey(true)
                .build()
        try {
            sharedTableBinaryHashKey.createBackup(CreateBackupRequest().withBackupName("foo")
                    .withTableName("bar"))
            fail("Should have failed trying to create backup with table name specified") as Unit
        } catch (ex: IllegalArgumentException) {
            assertTrue(ex.message!!.contains("table-name arguments are disallowed"), ex.message)
        }

        try {
            sharedTableBinaryHashKey.createBackup(CreateBackupRequest())
            fail("Should have failed trying to create backup without configuring a backup name") as Unit
        } catch (ex: NullPointerException) {
            assertTrue(ex.message!!.contains("Must pass backup name."), ex.message)
        }
    }

    /**
     * Validate params of list backup requests that are ignored in a multitenant backup manager are explicitly
     * disallowed.
     */
    @Test
    fun testListBackupsRequestInputValidation() {
        val s3Client: AmazonS3 = mock(AmazonS3::class.java)
        val dynamo = AmazonDynamoDbLocal.getAmazonDynamoDbLocal()
        val mtContext = MtAmazonDynamoDbContextProviderThreadLocalImpl()
        val sharedTableBinaryHashKey = SharedTableBuilder.builder()
                .withAmazonDynamoDb(dynamo)
                .withContext(mtContext)
                .withBackupSupport(s3Client, "fake-bucket", MtSharedTableBackupManagerS3It.MtScanningSnapshotter())
                .withTruncateOnDeleteTable(true)
                .withBinaryHashKey(true)
                .build()
        mtContext.withContext(null) {
            try {
                sharedTableBinaryHashKey.listBackups(ListBackupsRequest().withBackupType("foo"))
                fail("Should have failed trying to list backups of a particular type") as Unit
            } catch (ex: IllegalArgumentException) {
                assertTrue(ex.message!!.contains("Listing backups by backupType unsupported"))
            }

            try {
                sharedTableBinaryHashKey.listBackups(ListBackupsRequest().withTableName("foo"))
                fail("Should have failed trying to list backups of a particular table name") as Unit
            } catch (ex: IllegalArgumentException) {
                assertTrue(ex.message!!.contains("Listing backups by table name unsupported for multitenant backups"))
            }

            try {
                sharedTableBinaryHashKey.listBackups(ListBackupsRequest().withTimeRangeLowerBound(Date()))
                fail("Should have failed trying to list backups with time ranges") as Unit
            } catch (ex: IllegalArgumentException) {
                assertTrue(ex.message!!.contains("Listing backups filtered by time range unsupported"))
            }
            try {
                sharedTableBinaryHashKey.listBackups(ListBackupsRequest().withTimeRangeUpperBound(Date()))
                fail("Should have failed trying to list backups with time ranges") as Unit
            } catch (ex: IllegalArgumentException) {
                assertTrue(ex.message!!.contains("Listing backups filtered by time range unsupported"))
            }
        }
    }
}