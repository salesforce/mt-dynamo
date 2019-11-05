/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl

import com.amazonaws.SdkClientException
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.DeleteBackupRequest
import com.amazonaws.services.dynamodbv2.model.DescribeBackupRequest
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.ListBackupsRequest
import com.amazonaws.services.dynamodbv2.model.ListBackupsResult
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupRequest
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal
import com.salesforce.dynamodbv2.mt.backups.MtBackupException
import com.salesforce.dynamodbv2.mt.backups.MtBackupManager
import com.salesforce.dynamodbv2.mt.backups.MtBackupTableSnapshotter
import com.salesforce.dynamodbv2.mt.backups.MtScanningSnapshotter
import com.salesforce.dynamodbv2.mt.backups.Status
import com.salesforce.dynamodbv2.mt.backups.TenantTableBackupMetadata
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderThreadLocalImpl
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.TenantTable
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo
import com.salesforce.dynamodbv2.testsupport.ItemBuilder.HASH_KEY_FIELD
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.function.Supplier
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
        val backupArns = Lists.newArrayList<String>()

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
                    .withBackupSupport(s3, bucket, MtScanningSnapshotter())
                    .withTruncateOnDeleteTable(true)
                    .withBinaryHashKey(true)
                    .build()
            backupManager = sharedTableBinaryHashKey!!.backupManager
            try {
                backupManager!!.listBackups(ListBackupsRequest())
            } catch (e: SdkClientException) {
                fail<Unit>("Start local s3 with either " +
                        "`mvn verify -Ps3-integration-tests -Dskip.surefire.tests` from command line\n or " +
                        "`docker run -p 9090:9090 -p 9191:9191 -e initialBuckets=test-basic-backup-create -t adobe/s3mock:latest`")
            }
        }
    }

    @AfterEach
    fun afterEach() {
        for (backupArn in backupArns) {
            sharedTableBinaryHashKey!!.deleteBackup(DeleteBackupRequest().withBackupArn(backupArn))
            assertNull(backupManager!!.getBackup(backupArn))
        }
        backupArns.clear()
        assertTrue(backupManager!!.listBackups(ListBackupsRequest()).backupSummaries.isEmpty())
    }

    @Test
    fun testBasicBackupCreate_sameTenantNewTable() {
        val srcTenantTable = TenantTable(virtualTableName = "dummy-table", tenantName = "org1")
        val targetTenantTable = TenantTable(
                virtualTableName = srcTenantTable.virtualTableName + "-copy",
                tenantName = srcTenantTable.tenantName)
        basicBackupTest(srcTenantTable, targetTenantTable)
    }

    @Test
    fun testMultiTenantRestoreFails() {
        val tenantTable = TenantTable(virtualTableName = "table", tenantName = "tenant")
        createTestData(tenantTable)
        val backupName = "testMultiTenantRestoreFails"
        createBackup(backupName)
        // now try restoring backed up data to new target table and validate data appears on target
        val tenantTableBackups = MT_CONTEXT.withContext(
                tenantTable.tenantName,
                Supplier<ListBackupsResult> {
                    sharedTableBinaryHashKey!!
                            .listBackups(ListBackupsRequest()
                                    .withTableName(tenantTable.virtualTableName))
                })
        assertEquals(1, tenantTableBackups.backupSummaries.size)
        val backupArn = tenantTableBackups.backupSummaries[0].backupArn
        MT_CONTEXT.withContext(null) {
            try {
                sharedTableBinaryHashKey!!.restoreTableFromBackup(RestoreTableFromBackupRequest()
                        .withTargetTableName("table-copy")
                        .withBackupArn(backupArn))
                fail("Should not reach here") as Unit
            } catch (e: MtBackupException) {
                assertTrue(e.message!!.contains("Cannot do restore of backup without tenant specifier"))
            }
        }
    }

    private fun byteBufferToString(b: ByteBuffer) = String(b.array(), Charsets.UTF_8)

    private fun stringToByteBuffer(s: String) = ByteBuffer.wrap(s.toByteArray(Charsets.UTF_8))

    private fun createTestData(tenantTable: TenantTable): CreateTableRequest {
        val createdTableRequest = CreateTableRequestBuilder.builder()
                .withTableName(tenantTable.virtualTableName)
                .withAttributeDefinitions(AttributeDefinition(HASH_KEY_FIELD, ScalarAttributeType.B))
                .withKeySchema(KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withProvisionedThroughput(1L, 1L).build()
        MT_CONTEXT.withContext(tenantTable.tenantName) {
            sharedTableBinaryHashKey!!.createTable(createdTableRequest)
            sharedTableBinaryHashKey!!.putItem(PutItemRequest(tenantTable.virtualTableName,
                    ImmutableMap.of(HASH_KEY_FIELD, AttributeValue().withB(stringToByteBuffer("row1")), "value", AttributeValue("1"))))
            sharedTableBinaryHashKey!!.putItem(PutItemRequest(tenantTable.virtualTableName,
                    ImmutableMap.of(HASH_KEY_FIELD, AttributeValue().withB(stringToByteBuffer("row2")), "value", AttributeValue("2"))))
        }
        return createdTableRequest
    }

    private fun createBackup(backupName: String) {
        MT_CONTEXT.withContext(null) {
            sharedTableBinaryHashKey!!.createBackup(CreateBackupRequest()
                    .withBackupName(backupName))
            val mtBackupMetadata = backupManager!!.getBackup(backupName)
            assertNotNull(mtBackupMetadata)
            assertEquals(backupName, mtBackupMetadata!!.mtBackupName)
            assertEquals(Status.COMPLETE, mtBackupMetadata.status)
            assertTrue(mtBackupMetadata.tenantTables.isNotEmpty())
            backupArns.add(backupName)
        }
    }

    private fun basicBackupTest(sourceTenantTable: TenantTable, targetTenantTable: TenantTable) {
        // create dummy virtual table and dummy data to backup
        val createdTableRequest = createTestData(sourceTenantTable)
        // create backup of dummy data
        val backupName = "test-backup"

        createBackup(backupName)
        MT_CONTEXT.withContext(null) {
            val tenantBackupMetadata = backupManager!!.getTenantTableBackup(backupName, sourceTenantTable)
            assertNotNull(tenantBackupMetadata)
            assertEquals(backupName, tenantBackupMetadata!!.backupName)
            assertEquals(Status.COMPLETE, tenantBackupMetadata.status)
            assertEquals(sourceTenantTable, tenantBackupMetadata.tenantTable)
        }

        var backupArn: String? = null
        MT_CONTEXT.withContext(sourceTenantTable.tenantName) {
            val tenantTableBackups = sharedTableBinaryHashKey!!.listBackups(ListBackupsRequest().withTableName(sourceTenantTable.virtualTableName))
            assertEquals(1, tenantTableBackups.backupSummaries.size)
            backupArn = tenantTableBackups.backupSummaries[0].backupArn

            val tenantBackupArn = backupManager!!.getBackupArnForTenantTableBackup(
                    TenantTableBackupMetadata(bucket,
                            backupName,
                            sourceTenantTable.tenantName,
                            sourceTenantTable.virtualTableName))
            assertEquals(backupArn, tenantBackupArn)
            val tenantBackupDesc = sharedTableBinaryHashKey!!
                    .describeBackup(DescribeBackupRequest().withBackupArn(tenantBackupArn))
            assertNotNull(tenantBackupDesc.backupDescription)
            assertEquals(tenantBackupArn, tenantBackupDesc.backupDescription.backupDetails.backupArn)
            assertEquals(tenantTableBackups.backupSummaries[0].backupCreationDateTime,
                    tenantBackupDesc.backupDescription.backupDetails.backupCreationDateTime)
        }

        // now try restoring backed up data to new target table and validate data appears on target
        MT_CONTEXT.withContext(targetTenantTable.tenantName) {

            val restoreResult = sharedTableBinaryHashKey!!.restoreTableFromBackup(RestoreTableFromBackupRequest()
                    .withTargetTableName(targetTenantTable.virtualTableName)
                    .withBackupArn(backupArn))

            assertEquals(createdTableRequest.keySchema, restoreResult.tableDescription.keySchema)
            assertEquals(createdTableRequest.attributeDefinitions, restoreResult.tableDescription.attributeDefinitions)
            assertEquals(targetTenantTable.virtualTableName, restoreResult.tableDescription.tableName)

            val clonedRow = sharedTableBinaryHashKey!!.getItem(
                    GetItemRequest(targetTenantTable.virtualTableName, ImmutableMap.of(HASH_KEY_FIELD, AttributeValue().withB(stringToByteBuffer("row1")))))
            assertNotNull(clonedRow)
            assertNotNull(clonedRow.item)
            assertEquals("1", clonedRow.item["value"]!!.s)
        }
    }

    @Test
    fun testDifferentTenantRestoreSameTable() {
        basicBackupTest(
                TenantTable("table", "tenant-1"),
                TenantTable("table", "tenant-2"))
    }

    @Test
    fun testListBackups() {
        createOnlyBackupMetadataList("testListBackup", 3)
        val listBackupResult: ListBackupsResult = backupManager!!.listBackups(ListBackupsRequest())
        assertEquals(3, listBackupResult.backupSummaries.size)
    }

    @Test
    fun testListTenantBackups() {
        val tenant1 = "tenant-1"
        val tenant2 = "tenant-2"
        val table1 = "table-1"
        val table2 = "table-2"
        val createdTableRequestBuilder = CreateTableRequestBuilder.builder()
                .withAttributeDefinitions(AttributeDefinition(HASH_KEY_FIELD, ScalarAttributeType.S))
                .withKeySchema(KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withProvisionedThroughput(1L, 1L)
        val tenantTableMetadataList = ArrayList<MtTableDescriptionRepo.MtCreateTableRequest>()
        tenantTableMetadataList.add(MtTableDescriptionRepo.MtCreateTableRequest(tenantName = tenant1, createTableRequest = createdTableRequestBuilder.withTableName(table1).build()))

        createOnlyBackupMetadataList("testListBackup", 3, tenantTableMetadataList)
        tenantTableMetadataList.add(MtTableDescriptionRepo.MtCreateTableRequest(tenantName = tenant2, createTableRequest = createdTableRequestBuilder.withTableName(table2).build()))
        createOnlyBackupMetadataList("testListBackup-2", 3, tenantTableMetadataList)
        val listBackupResult1: ListBackupsResult = backupManager!!.listTenantTableBackups(ListBackupsRequest().withTableName(table1), tenant1)
        assertEquals(6, listBackupResult1.backupSummaries.size)
        val listBackupResult2: ListBackupsResult = backupManager!!.listTenantTableBackups(ListBackupsRequest().withTableName(table2), tenant2)
        assertEquals(3, listBackupResult2.backupSummaries.size)
    }

    @Test
    fun testListTenantBackups_empty() {
        val listBackupResult: ListBackupsResult = backupManager!!.listTenantTableBackups(ListBackupsRequest().withTableName("bar"), "foo")
        assertTrue(listBackupResult.backupSummaries.size == 0)
        assertNull(listBackupResult.lastEvaluatedBackupArn)
    }

    @Test
    fun testListTenantBackups_pagination() {
        val tenant = "tenant"
        val table = "table"
        val createdTableRequestBuilder = CreateTableRequestBuilder.builder()
                .withAttributeDefinitions(AttributeDefinition(HASH_KEY_FIELD, ScalarAttributeType.S))
                .withKeySchema(KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withProvisionedThroughput(1L, 1L)
        val tenantTableMetadataList = ArrayList<MtTableDescriptionRepo.MtCreateTableRequest>()
        tenantTableMetadataList.add(MtTableDescriptionRepo.MtCreateTableRequest(tenantName = tenant, createTableRequest = createdTableRequestBuilder.withTableName(table).build()))
        val backupIds = createOnlyBackupMetadataList("testListBackups_pagination", 7, ImmutableList.of())
        val expectedBackupIds = createOnlyBackupMetadataList("testListBackups_pagination2-", 7, tenantTableMetadataList)
        backupIds.addAll(expectedBackupIds)

        val limit = 4
        val firstResult = backupManager!!.listTenantTableBackups(ListBackupsRequest().withLimit(limit).withTableName(table), tenant)
        assertEquals(limit, firstResult.backupSummaries.size)
        assertEquals(expectedBackupIds.subList(0, limit),
                firstResult.backupSummaries.stream().map { s -> s.backupName }.collect(Collectors.toList()))
        assertNotNull(firstResult.lastEvaluatedBackupArn)
        val theRest = backupManager!!.listTenantTableBackups(ListBackupsRequest()
                .withLimit(limit)
                .withTableName(table)
                .withExclusiveStartBackupArn(firstResult.lastEvaluatedBackupArn), tenant)
        assertEquals(3, theRest.backupSummaries.size)
        assertEquals(expectedBackupIds.subList(4, 7),
                theRest.backupSummaries.stream().map { s -> s.backupName }.collect(Collectors.toList()))
        assertNull(theRest.lastEvaluatedBackupArn)
    }

    /**
     * Create only backup metadata list used to validate list backup tests, and return List of backup IDs created.
     */
    private fun createOnlyBackupMetadataList(backupPrefix: String, numBackups: Int, tenantTableMetadataList: List<MtTableDescriptionRepo.MtCreateTableRequest> = ImmutableList.of()): ArrayList<String> {
        val ret = Lists.newArrayList<String>()
        backupManager =
                object : MtSharedTableBackupManager(s3!!, bucket, sharedTableBinaryHashKey!!,
                        MtBackupTableSnapshotter()) {

                    // don't actually scan and backup metadata
                    override fun backupVirtualTableMetadata(
                        createBackupRequest: CreateBackupRequest
                    ): List<MtTableDescriptionRepo.MtCreateTableRequest> {
                        return tenantTableMetadataList
                    }
                }
        for (i in 1..numBackups) {
            val backupName = "$backupPrefix-$i"
            ret.add(backupName)
            val createBackupRequest = CreateBackupRequest()
            createBackupRequest.backupName = backupName

            backupManager!!.createBackup(createBackupRequest)
        }
        backupArns.addAll(ret)
        return ret
    }

    @Test
    fun testListBackups_empty() {
        val listBackupResult: ListBackupsResult = backupManager!!.listBackups(ListBackupsRequest())
        assertTrue(listBackupResult.backupSummaries.size == 0)
        assertNull(listBackupResult.lastEvaluatedBackupArn)
    }

    @Test
    fun testListBackups_pagination() {
        val backupIds = createOnlyBackupMetadataList("testListBackups_pagination", 7)

        val firstResult = backupManager!!.listBackups(ListBackupsRequest().withLimit(4))
        assertEquals(4, firstResult.backupSummaries.size)
        assertEquals(backupIds.subList(0, firstResult.backupSummaries.size),
                firstResult.backupSummaries.stream().map { s -> s.backupName }.collect(Collectors.toList()))
        assertNotNull(firstResult.lastEvaluatedBackupArn)
        val theRest = backupManager!!.listBackups(ListBackupsRequest()
                .withLimit(5)
                .withExclusiveStartBackupArn(firstResult.lastEvaluatedBackupArn))
        assertEquals(7 - firstResult.backupSummaries.size, theRest.backupSummaries.size)
        assertEquals(backupIds.subList(firstResult.backupSummaries.size, 7),
                theRest.backupSummaries.stream().map { s -> s.backupName }.collect(Collectors.toList()))
        assertNull(theRest.lastEvaluatedBackupArn)
    }
}