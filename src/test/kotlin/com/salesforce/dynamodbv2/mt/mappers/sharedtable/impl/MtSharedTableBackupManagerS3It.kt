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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest
import com.amazonaws.services.dynamodbv2.model.DeleteBackupRequest
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.ListBackupsRequest
import com.amazonaws.services.dynamodbv2.model.ListBackupsResult
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupRequest
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal
import com.salesforce.dynamodbv2.mt.backups.MtBackupException
import com.salesforce.dynamodbv2.mt.backups.MtBackupManager
import com.salesforce.dynamodbv2.mt.backups.MtBackupTableSnapshotter
import com.salesforce.dynamodbv2.mt.backups.SnapshotRequest
import com.salesforce.dynamodbv2.mt.backups.SnapshotResult
import com.salesforce.dynamodbv2.mt.backups.Status
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
import java.util.ArrayList
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

    private fun basicBackupTest(sourceTenantTable: TenantTable, targetTenantTable: TenantTable) {
        // create dummy virtual table and dummy data to backup
        val createdTableRequest = CreateTableRequestBuilder.builder()
                .withTableName(sourceTenantTable.virtualTableName)
                .withAttributeDefinitions(AttributeDefinition(HASH_KEY_FIELD, ScalarAttributeType.S))
                .withKeySchema(KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withProvisionedThroughput(1L, 1L).build()
        MtSharedTableBackupManagerS3It.MT_CONTEXT.withContext(sourceTenantTable.tenantName) {
            sharedTableBinaryHashKey!!.createTable(createdTableRequest)
            sharedTableBinaryHashKey!!.putItem(PutItemRequest(sourceTenantTable.virtualTableName,
                    ImmutableMap.of(HASH_KEY_FIELD, AttributeValue("row1"), "value", AttributeValue("1"))))
            sharedTableBinaryHashKey!!.putItem(PutItemRequest(sourceTenantTable.virtualTableName,
                    ImmutableMap.of(HASH_KEY_FIELD, AttributeValue("row2"), "value", AttributeValue("2"))))
        }

        // create backup of dummy data
        val backupName = "test-backup"

        MT_CONTEXT.withContext(null) {
            sharedTableBinaryHashKey!!.createBackup(CreateBackupRequest()
                    .withBackupName(backupName))
            val mtBackupMetadata = backupManager!!.getBackup(backupName)
            assertNotNull(mtBackupMetadata)
            assertEquals(backupName, mtBackupMetadata!!.mtBackupName)
            assertEquals(Status.COMPLETE, mtBackupMetadata.status)
            assertTrue(mtBackupMetadata.tenantTables.isNotEmpty())
            backupArns.add(backupName)

            val tenantBackupMetadata = backupManager!!.getTenantTableBackup(backupName, sourceTenantTable)
            assertNotNull(tenantBackupMetadata)
            assertEquals(backupName, tenantBackupMetadata!!.backupName)
            assertEquals(Status.COMPLETE, tenantBackupMetadata.status)
            assertEquals(sourceTenantTable, tenantBackupMetadata.tenantTable)
        }

        // now try restoring backed up data to new target table and validate data appears on target
        MT_CONTEXT.withContext(targetTenantTable.tenantName) {
            val tenantTableBackups = sharedTableBinaryHashKey!!.listBackups(ListBackupsRequest().withTableName(sourceTenantTable.virtualTableName))
            assertEquals(1, tenantTableBackups.backupSummaries.size)
            val restoreResult = sharedTableBinaryHashKey!!.restoreTableFromBackup(RestoreTableFromBackupRequest()
                    .withTargetTableName(targetTenantTable.virtualTableName)
                    .withBackupArn(tenantTableBackups.backupSummaries[0].backupArn))

            assertEquals(createdTableRequest.keySchema, restoreResult.tableDescription.keySchema)
            assertEquals(createdTableRequest.attributeDefinitions, restoreResult.tableDescription.attributeDefinitions)
            assertEquals(targetTenantTable.virtualTableName, restoreResult.tableDescription.tableName)

            val clonedRow = sharedTableBinaryHashKey!!.getItem(
                    GetItemRequest(targetTenantTable.virtualTableName, ImmutableMap.of(HASH_KEY_FIELD, AttributeValue("row1"))))
            assertNotNull(clonedRow)
            assertNotNull(clonedRow.item)
            assertEquals("1", clonedRow.item.get("value")!!.s)
        }
    }

    @Test
    fun testDifferentTenantRestoreFail() {
        val tenant = "tenant"
        val table = "table"
        val createdTableRequest = CreateTableRequestBuilder.builder()
                .withAttributeDefinitions(AttributeDefinition(HASH_KEY_FIELD, ScalarAttributeType.S))
                .withKeySchema(KeySchemaElement(HASH_KEY_FIELD, KeyType.HASH))
                .withTableName(table)
                .withProvisionedThroughput(1L, 1L).build()
        createOnlyBackupMetadataList("testDifferentTenantRestoreFail", 1, ImmutableList.of(MtTableDescriptionRepo.MtCreateTableRequest(tenant, createdTableRequest)))

        var tenantTableBackups: ListBackupsResult? = null
        MT_CONTEXT.withContext(tenant) {
            tenantTableBackups = sharedTableBinaryHashKey!!.listBackups(ListBackupsRequest().withTableName(table))
            assertEquals(1, tenantTableBackups!!.backupSummaries.size)
        }
        MT_CONTEXT.withContext("$tenant-2") {
            try {
                sharedTableBinaryHashKey!!.restoreTableFromBackup(RestoreTableFromBackupRequest()
                        .withTargetTableName("$table-2")
                        .withBackupArn(tenantTableBackups!!.backupSummaries[0].backupArn))
                fail("Should have failed trying to restore a different tenant context backup")
            } catch (e: MtBackupException) {
                assertTrue(e.message!!.contains("Error restoring table, invalid input."))
            }
        }
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
        assertEquals(expectedBackupIds.subList(0, firstResult.backupSummaries.size),
                firstResult.backupSummaries.stream().map { s -> s.backupName }.collect(Collectors.toList()))
        assertNotNull(firstResult.lastEvaluatedBackupArn)
        val theRest = backupManager!!.listTenantTableBackups(ListBackupsRequest()
                .withLimit(limit)
                .withTableName(table)
                .withExclusiveStartBackupArn(firstResult.lastEvaluatedBackupArn), tenant)
        assertEquals(7 - limit, theRest.backupSummaries.size)
        assertEquals(expectedBackupIds.subList(firstResult.backupSummaries.size, 7),
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
        assertTrue(firstResult.backupSummaries.size <= 4)
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

    /**
     * A mock table snapshotter that relies on naively scanning and copying table versus using on-demand backup features
     * unavailable on local dynamo.
     */
    class MtScanningSnapshotter : MtBackupTableSnapshotter() {
        override fun snapshotTableToTarget(snapshotRequest: SnapshotRequest): SnapshotResult {
            val startTime = System.currentTimeMillis()
            val sourceTableDescription: TableDescription = snapshotRequest.amazonDynamoDb
                    .describeTable(snapshotRequest.sourceTableName).table
            val createTableBuilder = CreateTableRequestBuilder.builder().withTableName(snapshotRequest.targetTableName)
                    .withKeySchema(*sourceTableDescription.keySchema.toTypedArray())

                    .withProvisionedThroughput(
                            snapshotRequest.targetTableProvisionedThroughput.readCapacityUnits,
                            snapshotRequest.targetTableProvisionedThroughput.writeCapacityUnits)
                    .withAttributeDefinitions(*sourceTableDescription.attributeDefinitions.stream().filter { a ->
                        sourceTableDescription.keySchema.stream().anyMatch { k -> a.attributeName.equals(k.attributeName) }
                    }.collect(Collectors.toList()).toTypedArray())
            snapshotRequest.amazonDynamoDb.createTable(createTableBuilder.build())

            var exclusiveStartKey: Map<String, AttributeValue>? = null

            do {
                var oldTableScanRequest: ScanRequest = ScanRequest().withTableName(snapshotRequest.sourceTableName)
                        .withExclusiveStartKey(exclusiveStartKey)
                val scanResult = snapshotRequest.amazonDynamoDb.scan(oldTableScanRequest)
                for (item in scanResult.items) {
                    snapshotRequest.amazonDynamoDb.putItem(snapshotRequest.targetTableName, item)
                }
                exclusiveStartKey = scanResult.lastEvaluatedKey
            } while (exclusiveStartKey != null)
            return SnapshotResult(snapshotRequest.mtBackupName,
                    snapshotRequest.targetTableName,
                    System.currentTimeMillis() - startTime)
        }

        override fun cleanup(snapshotResult: SnapshotResult, amazonDynamoDb: AmazonDynamoDB) {
            amazonDynamoDb.deleteTable(snapshotResult.tempSnapshotTable)
        }
    }
}