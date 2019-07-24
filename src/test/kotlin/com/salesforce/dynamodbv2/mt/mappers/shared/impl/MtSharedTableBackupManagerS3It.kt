/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.TenantTable
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder
import com.salesforce.dynamodbv2.mt.backups.CreateMtBackupRequest
import com.salesforce.dynamodbv2.mt.backups.ListMtBackupRequest
import com.salesforce.dynamodbv2.mt.backups.ListMtBackupsResult
import com.salesforce.dynamodbv2.mt.backups.MtBackupManager
import com.salesforce.dynamodbv2.mt.backups.RestoreMtBackupRequest
import com.salesforce.dynamodbv2.mt.backups.Status
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo
import com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.stream.Collectors

/**
 * Integration tests for mt-dynamo backup logic, integrated with mock s3. To run this, there are two options:
 *  - run `mvn verify -Ps3-integration-tests -Dskip.surefire.tests` from command line
 *  - run `docker run -p 9090:9090 -p 9191:9191 -e initialBuckets=test-basic-backup-create -t adobe/s3mock:latest`
 *      to start mock s3 sidecar docker container, and kick of these tests from an IDE
 */
internal class MtSharedTableBackupManagerS3It {

    companion object {
        val REGION = "us-east-1"
        val MT_CONTEXT: MtAmazonDynamoDbContextProvider = MtAmazonDynamoDbContextProviderThreadLocalImpl()

        val bucket: String = "test-basic-backup-create"
        val dynamo = AmazonDynamoDbLocal.getAmazonDynamoDbLocal()

        var sharedTableBinaryHashKey: MtAmazonDynamoDbBySharedTable? = null

        var backupManager: MtBackupManager? = null

        var s3: AmazonS3? = null

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            // operate against mock s3
            s3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials("foo", "bar")))
                    .withEndpointConfiguration(
                            AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:9090", REGION))
                    .build()

            sharedTableBinaryHashKey = SharedTableBuilder.builder()
                    .withAmazonDynamoDb(dynamo)
                    .withContext(MT_CONTEXT)
                    .withBackupSupport(s3, bucket)
                    .withTruncateOnDeleteTable(true)
                    .withBinaryHashKey(true)
                    .build()
            backupManager = sharedTableBinaryHashKey!!.backupManager
        }
    }

    @Test
    fun testBasicBackupCreate() {
        val tenant = "org1"
        val tableName = "dummy-table"

        val createdTableRequest = CreateTableRequestBuilder.builder()
                .withTableName(tableName)
                .withAttributeDefinitions(AttributeDefinition(HASH_KEY_FIELD, ScalarAttributeType.S))
                .withKeySchema(KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withProvisionedThroughput(1L, 1L).build()
        MT_CONTEXT.withContext(tenant) {
            sharedTableBinaryHashKey!!.createTable(createdTableRequest)
            sharedTableBinaryHashKey!!.putItem(PutItemRequest(tableName,
                    ImmutableMap.of(HASH_KEY_FIELD, AttributeValue("row1"), "value", AttributeValue("1"))))
            sharedTableBinaryHashKey!!.putItem(PutItemRequest(tableName,
                    ImmutableMap.of(HASH_KEY_FIELD, AttributeValue("row2"), "value", AttributeValue("2"))))
        }
        val backupId = "test-backup"

        try {
            MT_CONTEXT.withContext(null) {
                sharedTableBinaryHashKey!!.createBackup(CreateMtBackupRequest(backupId, shouldSnapshotTables = false))
                val mtBackupMetadata = backupManager!!.getBackup(backupId)
                assertNotNull(mtBackupMetadata)
                assertEquals(backupId, mtBackupMetadata!!.mtBackupId)
                assertEquals(Status.COMPLETE, mtBackupMetadata.status)
                assertTrue(mtBackupMetadata.tenantTables.size > 0)
            }
            val newRestoreTableName = tableName + "-copy"
            MT_CONTEXT.withContext(tenant) {
                val restoreResult = sharedTableBinaryHashKey!!.restoreTableFromBackup(
                        RestoreMtBackupRequest(backupId,
                                TenantTable(tenantName = tenant, virtualTableName = tableName),
                                TenantTable(tenantName = tenant, virtualTableName = newRestoreTableName)))

                assertEquals(createdTableRequest.keySchema, restoreResult.tableDescription.keySchema)
                assertEquals(createdTableRequest.attributeDefinitions, restoreResult.tableDescription.attributeDefinitions)
                assertEquals(newRestoreTableName, restoreResult.tableDescription.tableName)

                val clonedRow = sharedTableBinaryHashKey!!.getItem(
                        GetItemRequest(newRestoreTableName, ImmutableMap.of(HASH_KEY_FIELD, AttributeValue("row1"))))
                assertNotNull(clonedRow)
                assertNotNull(clonedRow.item)
                assertEquals("1", clonedRow.item.get("value")!!.s)
            }
        } finally {
            backupManager!!.deleteBackup(backupId)
            assertNull(backupManager!!.getBackup(backupId))
        }
    }

    @Test
    fun testListBackups() {
        val backupIds = createJustBackupMetadatas(3)
        try {

            val listBackupResult: ListMtBackupsResult = backupManager!!.listMtBackups(ListMtBackupRequest())
            assertTrue(listBackupResult.backupSummaries.size == 3)
        } finally {
            for (backup in backupIds) {
                backupManager!!.deleteBackup(backup)
                assertNull(backupManager!!.getBackup(backup))
            }
        }
    }

    /**
     * Create just backup metadatas used to validate list backup tests, and return List of backup ids created.
     */
    private fun createJustBackupMetadatas(numBackups: Int): List<String> {
        val ret = Lists.newArrayList<String>()
        backupManager =
                object : MtSharedTableBackupManager(s3!!, bucket, sharedTableBinaryHashKey!!) {

                    // don't actually scan and backup metadata
                    override fun backupVirtualTableMetadata(
                        createMtBackupRequest: CreateMtBackupRequest
                    ): List<MtTableDescriptionRepo.TenantTableMetadata> {
                        return ImmutableList.of()
                    }
                }
        for (i in 1..numBackups) {
            val backupId = "testListBackup-$i"
            ret.add(backupId)
            backupManager!!.createMtBackup(
                    CreateMtBackupRequest(backupId, shouldSnapshotTables = false))
        }
        return ret
    }

    @Test
    fun testListBackups_empty() {
        val listBackupResult: ListMtBackupsResult = backupManager!!.listMtBackups(ListMtBackupRequest())
        assertTrue(listBackupResult.backupSummaries.size == 0)
        assertNull(listBackupResult.lastEvaluatedBackupArn)
    }

    @Test
    fun testListBackups_pagination() {
        val backupIds: List<String> = createJustBackupMetadatas(7)
        try {
            val firstResult = backupManager!!.listMtBackups(ListMtBackupRequest(backupLimit = 4))
            assertTrue(firstResult.backupSummaries.size <= 4)
            assertEquals(backupIds.subList(0, firstResult.backupSummaries.size),
                    firstResult.backupSummaries.stream().map { s -> s.backupName }.collect(Collectors.toList()))
            assertNotNull(firstResult.lastEvaluatedBackup)
            val theRest = backupManager!!.listMtBackups(
                    ListMtBackupRequest(backupLimit = 5, exclusiveStartBackup = firstResult.lastEvaluatedBackup))
            assertEquals(7 - firstResult.backupSummaries.size, theRest.backupSummaries.size)
            assertEquals(backupIds.subList(3, 7),
                    theRest.backupSummaries.stream().map { s -> s.backupName }.collect(Collectors.toList()))
        } finally {
            for (backup in backupIds) {
                backupManager!!.deleteBackup(backup)
                assertNull(backupManager!!.getBackup(backup))
            }
        }
    }
}