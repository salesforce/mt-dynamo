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
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest
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
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.CreateMtBackupRequest
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.MtBackupManager
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.MtBackupMetadata
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.RestoreMtBackupRequest
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.Status
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo
import com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

internal class MtBackupManagerIt {

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
                sharedTableBinaryHashKey!!.createBackup(CreateBackupRequest().withBackupName(backupId))
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
        val backupIds = Lists.newArrayList<String>()
        backupManager =
                object : MtSharedTableBackupManagerImpl(s3!!, bucket) {

            // don't actually scan and backup metadata
            override fun backupVirtualTableMetadata(
                createMtBackupRequest: CreateMtBackupRequest,
                mtDynamo: MtAmazonDynamoDbBySharedTable
            ): List<MtTableDescriptionRepo.TenantTableMetadata> {
                return ImmutableList.of()
            }
        }
        try {
            for (i in 1..3) {
                val backupId = "testListBackup-$i"
                backupIds.add(backupId)
                backupManager!!.createMtBackup(
                        CreateMtBackupRequest(backupId), sharedTableBinaryHashKey!!)
            }

            val allBackups: List<MtBackupMetadata> = backupManager!!.listMtBackups()
            assertTrue(allBackups.size >= 3)
        } finally {
            for (backup in backupIds) {
                backupManager!!.deleteBackup(backup)
                assertNull(backupManager!!.getBackup(backup))
            }
        }
    }
}