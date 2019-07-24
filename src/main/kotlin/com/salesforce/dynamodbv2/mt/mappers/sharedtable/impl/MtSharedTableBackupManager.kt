/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.BackupSummary
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.DeleteObjectRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3Object
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import com.google.common.collect.Multimap
import com.google.common.collect.MultimapBuilder
import com.google.common.collect.Sets
import com.google.gson.ExclusionStrategy
import com.google.gson.FieldAttributes
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.TenantTable
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbBase
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo
import com.salesforce.dynamodbv2.mt.backups.CreateMtBackupRequest
import com.salesforce.dynamodbv2.mt.backups.ListMtBackupRequest
import com.salesforce.dynamodbv2.mt.backups.ListMtBackupsResult
import com.salesforce.dynamodbv2.mt.backups.MtBackupAwsAdaptor
import com.salesforce.dynamodbv2.mt.backups.MtBackupException
import com.salesforce.dynamodbv2.mt.backups.MtBackupManager
import com.salesforce.dynamodbv2.mt.backups.MtBackupMetadata
import com.salesforce.dynamodbv2.mt.backups.RestoreMtBackupRequest
import com.salesforce.dynamodbv2.mt.backups.Status
import com.salesforce.dynamodbv2.mt.backups.TenantRestoreMetadata
import com.salesforce.dynamodbv2.mt.backups.TenantTableBackupMetadata
import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.util.stream.Collectors

/**
 * Take an instance of {@link MtAmazonDynamoDbBySharedTable}, and using relatively naive scans across each table,
 * build a set of backups per tenant.
 */
open class MtSharedTableBackupManager(
    val s3: AmazonS3,
    val s3BucketName: String,
    val sharedTableMtDynamo: MtAmazonDynamoDbBySharedTable
) : MtBackupManager {

    private val logger = LoggerFactory.getLogger(MtSharedTableBackupManager::class.java)

    private val mtBackupAwsAdaptor = MtBackupAwsAdaptor()
    /**
     * At the moment, a naive (json) serializer is used to write and read backup snapshots.
     * This is wasteful in terms of space, but gets the job done.
     * Given CreateTableRequest objects are serialized to save table-tenant metadata, certain fields within
     * said object need to be filtered for GSON to deserialize the json back to a CreateTableRequest.
     *
     * Eventually, this should be replaced with a smarter [de]serialization strategy that is not as wasteful
     * in backup space cost.
     */
    val gson: Gson = GsonBuilder()
            .addDeserializationExclusionStrategy(object : ExclusionStrategy {
                override fun shouldSkipClass(clazz: Class<*>): Boolean {
                    return ignoreClassesOfCreateTableRequest.contains(clazz.name)
                }

                override fun shouldSkipField(field: FieldAttributes): Boolean {
                    return false
                }
            }).create()
    val ignoreClassesOfCreateTableRequest: Set<String> = ImmutableSet.of(
            "com.amazonaws.RequestClientOptions",
            "com.amazonaws.event.ProgressListener"
    )
    val charset = Charset.forName("utf-8")

    override fun createMtBackup(createMtBackupRequest: CreateMtBackupRequest): MtBackupMetadata {

        val backup = getBackup(createMtBackupRequest.backupId)
        if (backup != null) {
            throw MtBackupException("Backup with that ID already exists: $backup")
        } else {
            backupVirtualTableMetadata(createMtBackupRequest)
            val newMetadata = MtBackupMetadata(createMtBackupRequest.backupId,
                    Status.IN_PROGRESS,
                    ImmutableSet.of(),
                    System.currentTimeMillis())
            commitBackupMetadata(newMetadata)
            return getBackup(newMetadata.mtBackupId)!!
        }
    }

    open fun backupVirtualTableMetadata(
        createMtBackupRequest: CreateMtBackupRequest
    ): List<MtTableDescriptionRepo.TenantTableMetadata> {
        val startTime = System.currentTimeMillis()
        // write out table metadata
        val startKey: MtTableDescriptionRepo.TenantTableMetadata? = null
        val batchSize = 100
        var tenantTableCount = 0
        val tenantTables = Lists.newArrayList<MtTableDescriptionRepo.TenantTableMetadata>()
        do {
            val listTenantMetadataResult =
                    sharedTableMtDynamo.mtTableDescriptionRepo.listVirtualTableMetadata(
                            MtTableDescriptionRepo.ListMetadataRequest()
                                    .withExclusiveStartKey(startKey)
                                    .withLimit(batchSize))
            commitTenantTableMetadata(createMtBackupRequest.backupId, listTenantMetadataResult)
            tenantTables.addAll(listTenantMetadataResult.metadataList)
            tenantTableCount += listTenantMetadataResult.metadataList.size
        } while (listTenantMetadataResult.lastEvaluatedTable != null)
        logger.info("${createMtBackupRequest.backupId}: Finished generating backup metadata for " +
                "${tenantTables.size} virtual table in ${System.currentTimeMillis() - startTime} ms")
        return tenantTables
    }
    /**
     * Go through the shared data table and dump full row dumps into S3, segregated by tenant-table.
     *
     * Format on S3 looks like:
     *
     * ${backupBucket}/backupMetadata/metadata.json <- Overview of full backup and what is contained
     * ${backupBucket}/backupMetadata/${tenant1}/${virtTable1}-metadata.json <- CreateTableRequest tenant1-virtTable1
     * ${backupBucket}/backups/${tenant1}/${virtTable1}-${scanCount}.json
     * ${backupBucket}/backups/${tenant1}/${virtTable2}-${scanCount}.json
     * ...
     * ${backupBucket}/backups/${tenantN}/${virtTableN}-${N}}.json
     *
     * High level, the multitenant shared table is being scanned, and with each page of results, we write a file per
     * tenant-table onto S3 organized such that restoring a single tenant-table is a simple S3 directory listFiles
     * operation providing full row dumps to insert|restore back into dynamo under a different tenant-table space.
     *
     * @return an {@link MtBackupMetadata} object describing current metadata of backup with backed up tenant-tables.
     */
    override fun backupPhysicalMtTable(
        createMtBackupRequest: CreateMtBackupRequest,
        physicalTableName: String
    ): MtBackupMetadata {
        val startTime = System.currentTimeMillis()
        val backupMetadata = createBackupData(createMtBackupRequest, physicalTableName, sharedTableMtDynamo)
        // write out actual backup data
        commitBackupMetadata(backupMetadata)

        logger.info("${createMtBackupRequest.backupId}: Finished generating backup for " +
                "$physicalTableName in ${System.currentTimeMillis() - startTime} ms")
        return backupMetadata
    }

    override fun markBackupComplete(createMtBackupRequest: CreateMtBackupRequest): MtBackupMetadata {
        val inProgressackupMetadata = getBackup(createMtBackupRequest.backupId)
        if (inProgressackupMetadata == null || !inProgressackupMetadata.status.equals(Status.IN_PROGRESS)) {
            throw MtBackupException("Cannot mark $inProgressackupMetadata backup complete.")
        }
        val completedBackupMetadata = MtBackupMetadata(inProgressackupMetadata.mtBackupId,
                Status.COMPLETE,
                inProgressackupMetadata.tenantTables,
                inProgressackupMetadata.creationTime)
        commitBackupMetadata(completedBackupMetadata)
        return completedBackupMetadata
    }

    /**
     * Pull down the backup for the given tenant-table snapshot and do the following:
     *  - Issue a CreateTable command with the metadata of the tenant-table snapshot to the new tenant-table context
     *  - Go through each backup file, and insert each row serially into mt-dynamo with the new tenant-table context
     *
     *  There's a lot of room for improvement here, esp in parallelizing/bulkifying the restore operation.
     */
    override fun restoreTenantTableBackup(
        restoreMtBackupRequest: RestoreMtBackupRequest,
        mtDynamo: MtAmazonDynamoDbBase,
        mtContext: MtAmazonDynamoDbContextProvider
    ): TenantRestoreMetadata {
        val startTime = System.currentTimeMillis()
        // restore tenant-table metadata
        val createTableReq: CreateTableRequest = getTenantTableMetadata(restoreMtBackupRequest.backupId, restoreMtBackupRequest.tenantTableBackup)
        mtContext.withContext(restoreMtBackupRequest.newTenantTable.tenantName) {
            mtDynamo.createTable(createTableReq.withTableName(restoreMtBackupRequest.newTenantTable.virtualTableName))
        }

        // restore tenant-table data
        val backupFileKeys = getBackupFileKeys(restoreMtBackupRequest.backupId, restoreMtBackupRequest.tenantTableBackup)
        for (fileName in backupFileKeys) {
            val backupFile: S3Object = s3.getObject(s3BucketName, fileName)
            val rowsToInsert: List<TenantTableRow> = gson.fromJson(backupFile.objectContent.bufferedReader(),
                    object : TypeToken<List<TenantTableRow>>() {}.type)
            for (row in rowsToInsert) {
                mtContext.withContext(restoreMtBackupRequest.newTenantTable.tenantName) {
                    mtDynamo.putItem(restoreMtBackupRequest.newTenantTable.virtualTableName, row.attributeMap)
                }
            }
        }
        logger.info("${restoreMtBackupRequest.backupId}: Finished restoring ${restoreMtBackupRequest.tenantTableBackup}" +
                " to ${restoreMtBackupRequest.newTenantTable} in ${System.currentTimeMillis() - startTime} ms")

        return TenantRestoreMetadata(restoreMtBackupRequest.backupId,
                Status.COMPLETE,
                restoreMtBackupRequest.newTenantTable.tenantName,
                restoreMtBackupRequest.newTenantTable.virtualTableName)
    }

    override fun getTenantTableBackup(id: String): TenantTableBackupMetadata {
        TODO("not implemented")
    }

    private fun getTenantTableMetadata(backupId: String, tenantTable: TenantTable): CreateTableRequest {
        val tenantTableMetadataS3Location = getTenantTableMetadataFile(backupId, tenantTable)
        val tenantTableMetadataFile: S3Object = s3.getObject(s3BucketName, tenantTableMetadataS3Location)
        return gson.fromJson<CreateTableRequest>(tenantTableMetadataFile.objectContent.bufferedReader(),
                CreateTableRequest::class.java)
    }

    private fun getBackupFileKeys(backupId: String, tenantTable: TenantTable): Set<String> {
        val ret = Sets.newHashSet<String>()
        var continuationToken: String? = null
        do {
            val listBucketResult: ListObjectsV2Result = s3.listObjectsV2(
                    ListObjectsV2Request()
                            .withBucketName(s3BucketName)
                            .withContinuationToken(continuationToken)
                            .withPrefix("$backupDir/$backupId/${tenantTable.tenantName}/${tenantTable.virtualTableName}"))
            continuationToken = listBucketResult.continuationToken
            for (o in listBucketResult.objectSummaries) {
                ret.add(o.key)
            }
        } while (listBucketResult.isTruncated)
        return ret
    }

    override fun listMtBackups(listMtBackupRequest: ListMtBackupRequest): ListMtBackupsResult {
        val ret = Lists.newArrayList<BackupSummary>()
        val startAfter: String? = if (listMtBackupRequest.exclusiveStartBackup == null) null
            else getBackupMetadataFile(listMtBackupRequest.exclusiveStartBackup.backupName)
        val listBucketResult: ListObjectsV2Result = s3.listObjectsV2(
                ListObjectsV2Request()
                        .withMaxKeys(listMtBackupRequest.limit)
                        .withDelimiter("/")
                        .withBucketName(s3BucketName)
                        .withStartAfter(startAfter)
                        .withPrefix(backupMetadataDir + "/"))
        for (o in listBucketResult.objectSummaries) {
            val backup = mtBackupAwsAdaptor.getBackupSummary(getBackup(getBackupIdFromKey(o.key))!!)
            ret.add(backup)
        }
        var lastEvaluatedBackup: BackupSummary? = null
        if (listBucketResult.isTruncated) {
            lastEvaluatedBackup = Iterables.getLast(ret)
        }
        return ListMtBackupsResult(ret, lastEvaluatedBackup)
    }

    override fun getBackup(id: String): MtBackupMetadata? {
        try {
            val backupFile: S3Object = s3.getObject(s3BucketName, getBackupMetadataFile(id))
            return gson.fromJson(backupFile.objectContent.bufferedReader(),
                    MtBackupMetadata::class.java)
        } catch (e: AmazonS3Exception) {
            if ("NoSuchKey".equals(e.errorCode)) {
                return null
            } else {
                throw e
            }
        }
    }

    override fun deleteBackup(id: String): MtBackupMetadata? {
        var continuationToken: String? = null
        var deleteCount = 0
        do {
            val listBucketResult: ListObjectsV2Result = s3.listObjectsV2(
                    ListObjectsV2Request()
                            .withBucketName(s3BucketName)
                            .withContinuationToken(continuationToken)
                            .withPrefix("$backupDir/$id/"))
            continuationToken = listBucketResult.continuationToken
            if (listBucketResult.objectSummaries.size > 0) {
                deleteCount += listBucketResult.objectSummaries.size
                s3.deleteObjects(DeleteObjectsRequest(s3BucketName).withKeys(
                        listBucketResult.objectSummaries.stream()
                                .map { DeleteObjectsRequest.KeyVersion(it.key) }
                                .collect(Collectors.toList()) as List<DeleteObjectsRequest.KeyVersion>).withQuiet(true))
            }
        } while (listBucketResult.isTruncated)
        if (deleteCount > 0) {
            logger.info("$id: Deleted $deleteCount backup files for $id")
        }
        val ret = getBackup(id)
        s3.deleteObject(DeleteObjectRequest(s3BucketName, getBackupMetadataFile(id)))
        return ret
    }

    private fun commitTenantTableMetadata(
        backupId: String,
        tenantTableMetadatas: MtTableDescriptionRepo.ListMetadataResult
    ) {
        for (tenantTableMetadata in tenantTableMetadatas.metadataList) {
            val tenantTableMetadataJson = gson.toJson(tenantTableMetadata.createTableRequest).toByteArray(charset)
            val objectMetadata = ObjectMetadata()
            objectMetadata.contentLength = tenantTableMetadataJson.size.toLong()
            objectMetadata.contentType = "application/json"
            val putObjectReq = PutObjectRequest(s3BucketName,
                    getTenantTableMetadataFile(backupId, tenantTableMetadata.tenantTable),
                    gson.toJson(tenantTableMetadata.createTableRequest).byteInputStream(charset),
                    objectMetadata)
            s3.putObject(putObjectReq)
        }
    }

    protected fun commitBackupMetadata(backupMetadata: MtBackupMetadata) {
        val currentBackupMetadata = getBackup(backupMetadata.mtBackupId)

        val newBackupMetadata: MtBackupMetadata
        when (currentBackupMetadata) {
            null -> newBackupMetadata = backupMetadata
            else -> {
                if (currentBackupMetadata.status != Status.IN_PROGRESS) {
                    throw MtBackupException("${backupMetadata.mtBackupId} cannot continue, " +
                            " status is: ${currentBackupMetadata.status}")
                }
                newBackupMetadata = currentBackupMetadata.merge(backupMetadata)
            }
        }

        // write out metadata of what was backed up
        val backupJson = gson.toJson(newBackupMetadata)
        val backupMetadataBytes = backupJson.toByteArray(charset)
        val objectMetadata = ObjectMetadata()
        objectMetadata.contentLength = backupMetadataBytes.size.toLong()
        objectMetadata.contentType = "application/json"
        val putObjectReq = PutObjectRequest(s3BucketName, getBackupMetadataFile(newBackupMetadata.mtBackupId),
                backupJson.byteInputStream(charset), objectMetadata)
        s3.putObject(putObjectReq)
    }

    protected open fun createBackupData(
        createMtBackupRequest: CreateMtBackupRequest,
        physicalTableName: String,
        mtDynamo: MtAmazonDynamoDbBase
    ): MtBackupMetadata {
        var lastRow: TenantTableRow? = null
        val tenantTables = Sets.newHashSet<TenantTable>()
        var numPasses = 0
        do {
            val scanRequest: ScanRequest = ScanRequest(physicalTableName)
                    .withExclusiveStartKey(lastRow?.attributeMap)
            val scanResult = mtDynamo.scan(scanRequest)
            if (scanResult.lastEvaluatedKey != null) {
                lastRow = TenantTableRow(scanResult.lastEvaluatedKey)
            }

            val rowsPerTenant = MultimapBuilder.ListMultimapBuilder
                    .linkedHashKeys().hashSetValues().build<TenantTable, TenantTableRow>()
            for (i in 0..(scanResult.items.size - 1)) {
                val row = scanResult.items[i]
                val tenant = row.get(MtAmazonDynamoDbBase.DEFAULT_SCAN_TENANT_KEY)!!.s
                val virtualTable = row.get(MtAmazonDynamoDbBase.DEFAULT_SCAN_VIRTUAL_TABLE_KEY)!!.s
                rowsPerTenant.put(TenantTable(tenantName = tenant, virtualTableName = virtualTable),
                        TenantTableRow(scanResult.items[i]))
            }

            logger.info("Flushed ${rowsPerTenant.values().size} rows for " +
                    "${rowsPerTenant.keySet().size} tenants.")
            tenantTables.addAll(rowsPerTenant.keySet())
            flushScanResult(createMtBackupRequest.backupId, numPasses, rowsPerTenant)
        } while (scanResult.count > 0 && ++numPasses > 1)
        val tenantTableBackupMetadata = tenantTables
                .stream()
                .map {
                    TenantTableBackupMetadata(createMtBackupRequest.backupId,
                            Status.COMPLETE, it.tenantName, it.virtualTableName,
                            getBackupFileKeys(createMtBackupRequest.backupId, it))
                }
                .collect(Collectors.toSet())
        return MtBackupMetadata(createMtBackupRequest.backupId, Status.IN_PROGRESS,
                tenantTableBackupMetadata)
    }

    private val backupMetadataDir = "backupMetadata"
    private val backupDir = "backups"

    private fun getBackupMetadataFile(backupId: String) = "$backupMetadataDir/$backupId-metadata.json"
    private fun getBackupFile(backupId: String, tenantTable: TenantTable, scanCount: Int) =
            "$backupDir/$backupId/${tenantTable.tenantName}/${tenantTable.virtualTableName}-$scanCount.json"
    private fun getTenantTableMetadataFile(backupId: String, tenantTable: TenantTable) =
            "$backupMetadataDir/$backupId/${tenantTable.tenantName}/${tenantTable.virtualTableName}-metadata.json"

    private fun getBackupIdFromKey(backupMetadataKey: String) =
            backupMetadataKey.split("/")[1].split("-metadata")[0]

    private fun flushScanResult(backupId: String, scanCount: Int, rowsPerTenant: Multimap<TenantTable, TenantTableRow>) {
        for (tenantTable: TenantTable in rowsPerTenant.keySet()) {
            val toFlush = gson.toJson(rowsPerTenant.get(tenantTable))
            val objectMetadata = ObjectMetadata()
            objectMetadata.contentLength = toFlush.toByteArray(charset).size.toLong()
            objectMetadata.contentType = "application/json"
            val putObjectReq = PutObjectRequest(s3BucketName, getBackupFile(backupId, tenantTable, scanCount),
                    toFlush.byteInputStream(charset), objectMetadata)
            s3.putObject(putObjectReq)
        }
    }
}

data class TenantTableRow(val attributeMap: Map<String, AttributeValue>)

class MtSharedTableBackupManagerBuilder(val s3: AmazonS3, val s3BucketName: String) {
    fun build(sharedTableMtDynamo: MtAmazonDynamoDbBySharedTable): MtSharedTableBackupManager {
        return MtSharedTableBackupManager(s3, s3BucketName, sharedTableMtDynamo)
    }
}