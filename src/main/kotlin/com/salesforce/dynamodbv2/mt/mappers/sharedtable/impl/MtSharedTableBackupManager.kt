/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.BackupSummary
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription
import com.amazonaws.services.dynamodbv2.model.ListBackupsRequest
import com.amazonaws.services.dynamodbv2.model.ListBackupsResult
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription
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
import com.google.common.base.Preconditions
import com.google.common.base.Strings
import com.google.common.collect.Iterables
import com.google.common.collect.Multimap
import com.google.common.collect.MultimapBuilder
import com.google.common.io.CharStreams
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import com.salesforce.dynamodbv2.mt.backups.GsonByteBufferTypeAdapter
import com.salesforce.dynamodbv2.mt.backups.MtBackupAwsAdaptor
import com.salesforce.dynamodbv2.mt.backups.MtBackupException
import com.salesforce.dynamodbv2.mt.backups.MtBackupManager
import com.salesforce.dynamodbv2.mt.backups.MtBackupMetadata
import com.salesforce.dynamodbv2.mt.backups.MtBackupTableSnapshotter
import com.salesforce.dynamodbv2.mt.backups.RestoreMtBackupRequest
import com.salesforce.dynamodbv2.mt.backups.Status
import com.salesforce.dynamodbv2.mt.backups.TenantBackupMetadata
import com.salesforce.dynamodbv2.mt.backups.TenantRestoreMetadata
import com.salesforce.dynamodbv2.mt.backups.TenantTableBackupMetadata
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.TenantTable
import com.salesforce.dynamodbv2.mt.repo.MtTableDescription
import com.salesforce.dynamodbv2.mt.repo.MtTableDescriptionRepo
import org.slf4j.LoggerFactory
import java.io.InputStreamReader
import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.stream.Collectors

/**
 * Take an instance of {@link MtAmazonDynamoDbBySharedTable}, and using relatively naive scans across each table,
 * build a set of backups per tenant.
 */
open class MtSharedTableBackupManager(
    val s3: AmazonS3,
    val s3BucketName: String,
    val sharedTableMtDynamo: MtAmazonDynamoDbBySharedTable,
    val tableSnapshotter: MtBackupTableSnapshotter
) : MtBackupManager {
    private val logger = LoggerFactory.getLogger(MtSharedTableBackupManager::class.java)

    private val mtBackupAwsAdaptor = MtBackupAwsAdaptor()
    /**
     * At the moment, a naive (JSON) serializer is used to write and read backup snapshots.
     * This is wasteful in terms of space, but gets the job done.
     *
     * Eventually, this should be replaced with a smarter serialization/deserialization strategy that is not as wasteful
     * in terms of backup-space cost.
     */
    val gson: Gson = GsonBuilder()
            .enableComplexMapKeySerialization()
            .registerTypeAdapter(ByteBuffer::class.java, GsonByteBufferTypeAdapter())
            .create()
    val charset = Charsets.UTF_8

    override fun createBackup(createBackupRequest: CreateBackupRequest): MtBackupMetadata {

        val backup = getBackup(createBackupRequest.backupName)
        if (backup != null) {
            throw MtBackupException("Backup with that ID already exists: $backup")
        } else {
            val startTime = System.currentTimeMillis()
            val virtualMetadata = backupVirtualTableMetadata(createBackupRequest)
            val tenantTableCounts: Map<TenantTableBackupMetadata, Long> = virtualMetadata
                    .map { metadata ->
                        TenantTableBackupMetadata(
                                s3BucketName,
                                createBackupRequest.backupName,
                                metadata.tenantName,
                                metadata.tableDescription.tableName)
                    }
                    .associateBy({ it }, { 0L })
            val newMetadata = MtBackupMetadata(s3BucketName, createBackupRequest.backupName,
                    Status.IN_PROGRESS, tenantTableCounts, startTime)

            commitBackupMetadata(newMetadata)
            return getBackup(newMetadata.mtBackupName)!!
        }
    }

    open fun backupVirtualTableMetadata(
        createBackupRequest: CreateBackupRequest
    ): List<MtTableDescriptionRepo.MtTenantTableDesciption> {
        val startTime = System.currentTimeMillis()
        // write out table metadata
        val startKey: TenantTable? = null
        val batchSize = 100
        var tenantTableCount = 0
        val tenantTables = arrayListOf<MtTableDescriptionRepo.MtTenantTableDesciption>()
        do {
            val listTenantMetadataResult =
                    sharedTableMtDynamo.mtTableDescriptionRepo.listVirtualTableMetadata(
                            MtTableDescriptionRepo.ListMetadataRequest()
                                    .withExclusiveStartKey(startKey)
                                    .withLimit(batchSize))
            commitTenantTableMetadata(createBackupRequest.backupName, listTenantMetadataResult)
            tenantTables.addAll(listTenantMetadataResult.tenantTableDescriptions)
            tenantTableCount += listTenantMetadataResult.tenantTableDescriptions.size
        } while (listTenantMetadataResult.lastEvaluatedTable != null)
        logger.info("${createBackupRequest.backupName}: Finished generating backup metadata for " +
                "${tenantTables.size} virtual table in ${System.currentTimeMillis() - startTime} ms")
        return tenantTables
    }

    /**
     * Go through the shared data table and dump full row dumps into S3, segregated by tenant-table.
     *
     * Format on S3 looks like:
     *
     * ${backupBucket}/backupMetadata/metadata.json <- Overview of full backup and what is contained
     * ${backupBucket}/backupMetadata/${tenant1}/${virtTable1}-metadata.json <- MtTableDescription tenant1-virtTable1
     * ${backupBucket}/backups/${tenant1}/${virtTable1}-${scanCount}.json
     * ${backupBucket}/backups/${tenant1}/${virtTable2}-${scanCount}.json
     * ...
     * ${backupBucket}/backups/${tenantN}/${virtTableN}-${N}}.json
     *
     * At a high level, the multitenant shared table is being scanned, and with each page of results, we write a file
     * per tenant-table onto S3 organized such that restoring a single tenant-table is a simple S3 directory listFiles
     * operation providing full row dumps to insert|restore back into dynamo under a different tenant-table space.
     *
     * @return an {@link MtBackupMetadata} object describing current metadata of backup with backed up tenant-tables.
     */
    override fun backupPhysicalMtTable(
        createBackupRequest: CreateBackupRequest,
        physicalTableName: String
    ): MtBackupMetadata {
        val startTime = System.currentTimeMillis()
        val backupMetadata = createBackupData(createBackupRequest, physicalTableName)
        // write out actual backup data
        commitBackupMetadata(backupMetadata)

        logger.info("${createBackupRequest.backupName}: Finished generating backup for " +
                "$physicalTableName in ${System.currentTimeMillis() - startTime} ms")
        return backupMetadata
    }

    override fun getMtBackupTableSnapshotter(): MtBackupTableSnapshotter {
        return tableSnapshotter
    }

    override fun markBackupComplete(createBackupRequest: CreateBackupRequest): MtBackupMetadata {
        val inProgressBackupMetadata = getBackup(createBackupRequest.backupName)
        if (inProgressBackupMetadata == null || inProgressBackupMetadata.status != Status.IN_PROGRESS) {
            throw MtBackupException("Cannot mark $inProgressBackupMetadata backup complete.")
        }

        val completedBackupMetadata = MtBackupMetadata(s3BucketName, inProgressBackupMetadata.mtBackupName,
                Status.COMPLETE,
                inProgressBackupMetadata.tenantTables,
                inProgressBackupMetadata.creationTime)
        commitBackupMetadata(completedBackupMetadata)
        return completedBackupMetadata
    }

    /**
     * Pull down the backup for the given tenant-table snapshot and do the following:
     *  - Issue a CreateTable command with the metadata of the tenant-table snapshot to the new tenant-table context
     *  - Go through each backup file and insert each row serially into mt-dynamo with the new tenant-table context
     *
     *  There is a lot of room for improvement here, especially in parallelizing/bulkifying the restore operation.
     */
    override fun restoreTenantTableBackup(
        restoreMtBackupRequest: RestoreMtBackupRequest,
        mtContext: MtAmazonDynamoDbContextProvider
    ): TenantRestoreMetadata {
        val startTime = System.currentTimeMillis()
        // restore tenant-table metadata, unless it's a multitenant table, which we assume to be already created
        val tableDescription: MtTableDescription = getTenantTableMetadata(restoreMtBackupRequest.backupName,
                restoreMtBackupRequest.tenantTableBackup)
        if (!tableDescription.isMultitenant) {
            val createTableReq = toCreateTableRequest(tableDescription)
                .withTableName(restoreMtBackupRequest.newTenantTable.virtualTableName)
            mtContext.withContext(restoreMtBackupRequest.newTenantTable.tenantName) {
                sharedTableMtDynamo.createTable(createTableReq)
            }
        }

        // restore tenant-table data
        val backupFileKeys = getBackupFileKeys(restoreMtBackupRequest.backupName,
                restoreMtBackupRequest.tenantTableBackup)
        for (fileName in backupFileKeys) {
            val backupFile: S3Object = s3.getObject(s3BucketName, fileName)
            val rowsToInsert: List<TenantTableRow> = gson.fromJson(backupFile.objectContent.bufferedReader(),
                    object : TypeToken<List<TenantTableRow>>() {}.type)
            for (row in rowsToInsert) {
                mtContext.withContext(restoreMtBackupRequest.newTenantTable.tenantName) {
                    sharedTableMtDynamo.putItem(restoreMtBackupRequest.newTenantTable.virtualTableName, row.attributeMap)
                }
            }
        }
        logger.info("${restoreMtBackupRequest.backupName}: Finished restoring ${restoreMtBackupRequest.tenantTableBackup}" +
                " to ${restoreMtBackupRequest.newTenantTable} in ${System.currentTimeMillis() - startTime} ms")

        return TenantRestoreMetadata(restoreMtBackupRequest.backupName,
                Status.COMPLETE,
                restoreMtBackupRequest.newTenantTable.tenantName,
                restoreMtBackupRequest.newTenantTable.virtualTableName)
    }

    private fun getTenantTableMetadata(backupId: String, tenantTable: TenantTable): MtTableDescription {
        val tenantTableMetadataS3Location = getTenantTableMetadataFile(backupId, tenantTable)
        val tenantTableMetadataFile: S3Object = s3.getObject(s3BucketName, tenantTableMetadataS3Location)
        return gson.fromJson(tenantTableMetadataFile.objectContent.bufferedReader(),
                MtTableDescription::class.java)
    }

    private fun toCreateTableRequest(description: MtTableDescription): CreateTableRequest {
        return CreateTableRequest().withTableName(description.tableName)
                .withKeySchema(description.keySchema)
                .withAttributeDefinitions(description.attributeDefinitions)
                .withStreamSpecification(description.streamSpecification)
                .withProvisionedThroughput(toProvisionedThroughput(description.provisionedThroughput))
                .withGlobalSecondaryIndexes(toGlobalIndexes(description.globalSecondaryIndexes))
                .withLocalSecondaryIndexes(toLocalIndexes(description.localSecondaryIndexes))
    }

    private fun toProvisionedThroughput(description: ProvisionedThroughputDescription?): ProvisionedThroughput? {
        return if (description == null) null else ProvisionedThroughput(description.readCapacityUnits, description.writeCapacityUnits)
    }

    private fun toGlobalIndexes(descriptions: Collection<GlobalSecondaryIndexDescription>?): List<GlobalSecondaryIndex>? {
        return descriptions?.map { gsi -> GlobalSecondaryIndex().withIndexName(gsi.indexName)
                .withKeySchema(gsi.keySchema)
                .withProjection(gsi.projection)
                .withProvisionedThroughput(toProvisionedThroughput(gsi.provisionedThroughput)) }
    }

    private fun toLocalIndexes(descriptions: Collection<LocalSecondaryIndexDescription>?): List<LocalSecondaryIndex>? {
        return descriptions?.map { gsi -> LocalSecondaryIndex().withIndexName(gsi.indexName)
                .withKeySchema(gsi.keySchema)
                .withProjection(gsi.projection) }
    }

    private fun getBackupFileKeys(backupId: String, tenantTable: TenantTable): Set<String> {
        val ret = hashSetOf<String>()
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

    override fun listBackups(listBackupRequest: ListBackupsRequest): ListBackupsResult {
        val ret = arrayListOf<BackupSummary>()

        Preconditions.checkArgument(listBackupRequest.backupType == null, "Listing backups by backupType unsupported")
        Preconditions.checkArgument(listBackupRequest.tableName == null, "Listing backups by table name unsupported for multitenant backups")
        Preconditions.checkArgument(listBackupRequest.timeRangeLowerBound == null, "Listing backups filtered by time range unsupported [currently]")
        Preconditions.checkArgument(listBackupRequest.timeRangeUpperBound == null, "Listing backups filtered by time range unsupported [currently]")

        // translate the last backupArn we saw to the file name on S3, to iterate from
        var startAfter: String? = if (listBackupRequest.exclusiveStartBackupArn == null) null
            else getBackupMetadataFile(listBackupRequest.exclusiveStartBackupArn)
        var lastEvaluatedBackupArn: String? = null
        var continuationToken: String? = null
        val limit = if (listBackupRequest.limit == null) 10 else listBackupRequest.limit
        do {
            val listBucketResult: ListObjectsV2Result = s3.listObjectsV2(
                    ListObjectsV2Request()
                            .withMaxKeys(limit)
                            .withDelimiter("/")
                            .withContinuationToken(continuationToken)
                            .withBucketName(s3BucketName)
                            .withStartAfter(startAfter)
                            .withPrefix("$backupMetadataDir/"))
            for (o in listBucketResult.objectSummaries) {
                if (ret.size < limit) {
                    val backup = mtBackupAwsAdaptor.getBackupSummary(getBackup(getBackupIdFromKey(o.key))!!)
                    ret.add(backup)
                }
            }
            continuationToken = listBucketResult.nextContinuationToken
            if (ret.size == limit) {
                lastEvaluatedBackupArn = Iterables.getLast(ret).backupArn
            }
        } while (!Strings.isNullOrEmpty(continuationToken) && ret.size < limit)

        return ListBackupsResult().withBackupSummaries(ret)
                .withLastEvaluatedBackupArn(lastEvaluatedBackupArn)
    }

    override fun listTenantTableBackups(listBackupRequest: ListBackupsRequest, tenantId: String): ListBackupsResult {
        Preconditions.checkNotNull(tenantId, "Must pass tenant identifier")
        Preconditions.checkNotNull(listBackupRequest.tableName, "Must pass virtual table name")
        val mtBackupRequest = ListBackupsRequest().withLimit(listBackupRequest.limit)
        if (listBackupRequest.exclusiveStartBackupArn != null) {
            mtBackupRequest.withExclusiveStartBackupArn(getTenantTableBackupFromArn(listBackupRequest.exclusiveStartBackupArn).backupName)
        }
        var mtBackups: ListBackupsResult
        val ret = ArrayList<BackupSummary>()
        do {
            mtBackups = listBackups(mtBackupRequest)
            for (backup in mtBackups.backupSummaries) {
                val tenantTableBackup = getTenantTableBackup(backup.backupName, TenantTable(listBackupRequest.tableName, tenantId))
                if (tenantTableBackup != null) {
                    val tenantTableBackupArn = getBackupArnForTenantTableBackup(
                            TenantTableBackupMetadata(s3BucketName, tenantTableBackup.backupName, tenantTableBackup.tenantTable.tenantName, tenantTableBackup.tenantTable.virtualTableName))
                    ret.add(mtBackupAwsAdaptor.getBackupSummary(tenantTableBackup, tenantTableBackupArn))
                    if (ret.size == listBackupRequest.limit) break
                }
            }
            mtBackupRequest.withExclusiveStartBackupArn(mtBackups.lastEvaluatedBackupArn)
        } while (mtBackups.lastEvaluatedBackupArn != null && ret.size < listBackupRequest.limit)
        val listEvaluatedBackupArn: String? = if (mtBackups.lastEvaluatedBackupArn != null || ret.size == listBackupRequest.limit) Iterables.getLast(ret).backupArn else null
        return ListBackupsResult().withBackupSummaries(ret).withLastEvaluatedBackupArn(listEvaluatedBackupArn)
    }

    override fun getBackup(backupName: String): MtBackupMetadata? {
        return try {
            val backupFile: S3Object = s3.getObject(s3BucketName, getBackupMetadataFile(backupName))
            val backupFileString = CharStreams.toString(
                    InputStreamReader(backupFile.objectContent.delegateStream, charset))
            gson.fromJson(backupFileString,
                    MtBackupMetadata::class.java)
        } catch (e: AmazonS3Exception) {
            if ("NoSuchKey" == e.errorCode) {
                null
            } else {
                throw e
            }
        }
    }

    override fun getTenantTableBackup(backupName: String, tenantTable: TenantTable): TenantBackupMetadata? {
        val mtBackup = getBackup(backupName)

        if (mtBackup != null) {
            val tenantTableBackupMetadata = TenantTableBackupMetadata(mtBackup.s3BucketName, backupName, tenantTable.tenantName, tenantTable.virtualTableName)
            if (mtBackup.tenantTables.containsKey(tenantTableBackupMetadata)) {
                return TenantBackupMetadata(mtBackup.s3BucketName, tenantTable, backupName, mtBackup.status, mtBackup.creationTime)
            }
        }
        return null
    }

    override fun deleteBackup(backupName: String): MtBackupMetadata? {
        var continuationToken: String? = null
        var deleteCount = 0
        do {
            val listBucketResult: ListObjectsV2Result = s3.listObjectsV2(
                    ListObjectsV2Request()
                            .withBucketName(s3BucketName)
                            .withContinuationToken(continuationToken)
                            .withPrefix("$backupDir/$backupName/"))
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
            logger.info("$backupName: Deleted $deleteCount backup files for $backupName")
        }
        val ret = getBackup(backupName)
        s3.deleteObject(DeleteObjectRequest(s3BucketName, getBackupMetadataFile(backupName)))
        return ret
    }

    override fun getTenantTableBackupFromArn(backupArn: String): TenantTableBackupMetadata = mtBackupAwsAdaptor.getTenantTableBackupFromArn(backupArn)

    override fun getBackupArnForTenantTableBackup(tenantTable: TenantTableBackupMetadata): String = mtBackupAwsAdaptor.getBackupArnForTenantTableBackup(tenantTable)

    private fun commitTenantTableMetadata(
        backupId: String,
        tenantTableMetadataList: MtTableDescriptionRepo.ListMetadataResult
    ) {
        for (tenantTableMetadata in tenantTableMetadataList.tenantTableDescriptions) {
            val tenantTableMetadataJson = gson.toJson(tenantTableMetadata.tableDescription).toByteArray(charset)
            val objectMetadata = ObjectMetadata()
            objectMetadata.contentLength = tenantTableMetadataJson.size.toLong()
            objectMetadata.contentType = "application/json"
            val tenantTableMetadataFile = getTenantTableMetadataFile(backupId, TenantTable(tenantTableMetadata.tableDescription.tableName, tenantTableMetadata.tenantName))
            val putObjectReq = PutObjectRequest(s3BucketName,
                    tenantTableMetadataFile,
                    gson.toJson(tenantTableMetadata.tableDescription).byteInputStream(charset),
                    objectMetadata)
            s3.putObject(putObjectReq)
        }
    }

    /**
     * This method is a fragile component of this backup manager, as we are managing state of a backup
     * from S3, losing transactionality, and creating a file that is mutated from different threads.
     *
     * By marking this synchronized, we're delaying the problem of moving this metadata to a database, like Dynamo,
     * instead of using a JSON file on S3 to manage state. This will enforce only a single thread is updating
     * a backup metadata at a time, and will work as long as only a single JVM is participating
     * in updating a backups metadata.
     */
    @Synchronized
    protected fun commitBackupMetadata(backupMetadata: MtBackupMetadata) {
        val currentBackupMetadata = getBackup(backupMetadata.mtBackupName)

        val newBackupMetadata: MtBackupMetadata
        newBackupMetadata = when (currentBackupMetadata) {
            null -> backupMetadata
            else -> {
                if (currentBackupMetadata.status != Status.IN_PROGRESS) {
                    throw MtBackupException("${backupMetadata.mtBackupName} cannot continue, " +
                            " status is: ${currentBackupMetadata.status}")
                }
                currentBackupMetadata.merge(backupMetadata)
            }
        }

        // write out metadata of what was backed up
        val backupJson = gson.toJson(newBackupMetadata)
        val backupMetadataBytes = backupJson.toByteArray(charset)
        val objectMetadata = ObjectMetadata()
        objectMetadata.contentLength = backupMetadataBytes.size.toLong()
        objectMetadata.contentType = "application/json"
        val putObjectReq = PutObjectRequest(s3BucketName, getBackupMetadataFile(newBackupMetadata.mtBackupName),
                backupJson.byteInputStream(charset), objectMetadata)
        s3.putObject(putObjectReq)
    }

    protected open fun createBackupData(
        createBackupRequest: CreateBackupRequest,
        physicalTableName: String
    ): MtBackupMetadata {
        var lastRow: TenantTableRow? = null
        val tenantTables = hashMapOf<TenantTableBackupMetadata, Long>()
        var numPasses = 0
        do {
            val scanRequest: ScanRequest = ScanRequest(physicalTableName)
                    .withExclusiveStartKey(lastRow?.attributeMap)
            val scanResult = sharedTableMtDynamo.scanAllTenantsOfBackupSnapshotTable(scanRequest)
            if (scanResult.lastEvaluatedKey != null) {
                lastRow = TenantTableRow(scanResult.lastEvaluatedKey)
            }

            val rowsPerTenant = MultimapBuilder.ListMultimapBuilder
                    .linkedHashKeys().hashSetValues().build<TenantTable, TenantTableRow>()
            for (i in 0 until scanResult.items.size) {
                val row = scanResult.items[i]
                val tenant = row[sharedTableMtDynamo.scanTenantKey]!!.s
                val virtualTable = row[sharedTableMtDynamo.scanVirtualTableKey]!!.s
                row.remove(sharedTableMtDynamo.scanTenantKey)
                row.remove(sharedTableMtDynamo.scanVirtualTableKey)
                rowsPerTenant.put(TenantTable(tenantName = tenant, virtualTableName = virtualTable),
                        TenantTableRow(scanResult.items[i]))
            }

            flushScanResult(createBackupRequest.backupName, numPasses, rowsPerTenant)
            logger.info("Flushed ${rowsPerTenant.values().size} rows for " +
                    "${rowsPerTenant.keySet().size} tenants.")
            for (tenantTable in rowsPerTenant.keySet()) {
                val tenantTableMetadata = TenantTableBackupMetadata(s3BucketName = s3BucketName, tenantId = tenantTable.tenantName, virtualTableName = tenantTable.virtualTableName, backupName = createBackupRequest.backupName)
                tenantTables[tenantTableMetadata] = tenantTables.getOrDefault(tenantTableMetadata, 1L)
            }
        } while (scanResult.count > 0 && ++numPasses > 1)

        return MtBackupMetadata(s3BucketName, createBackupRequest.backupName, Status.IN_PROGRESS,
                tenantTables)
    }

    private val backupMetadataDir = "backupMetadata"
    private val backupDir = "backups"

    private fun getBackupMetadataFile(backupName: String) = "$backupMetadataDir/$backupName-metadata.json"
    private fun getBackupFile(backupName: String, tenantTable: TenantTable, scanCount: Int) =
            "$backupDir/$backupName/${tenantTable.tenantName}/${tenantTable.virtualTableName}-$scanCount.json"

    private fun getTenantTableMetadataFile(backupName: String, tenantTable: TenantTable) =
            "$backupMetadataDir/$backupName/${tenantTable.tenantName}/${tenantTable.virtualTableName}-metadata.json"

    private fun getBackupIdFromKey(backupMetadataKey: String) =
            backupMetadataKey.split("/")[1].split("-metadata")[0]

    private fun flushScanResult(backupName: String, scanCount: Int, rowsPerTenant: Multimap<TenantTable, TenantTableRow>) {
        for (tenantTable: TenantTable in rowsPerTenant.keySet()) {
            val toFlush = gson.toJson(rowsPerTenant.get(tenantTable))
            val objectMetadata = ObjectMetadata()
            objectMetadata.contentLength = toFlush.toByteArray(charset).size.toLong()
            objectMetadata.contentType = "application/json"
            val putObjectReq = PutObjectRequest(s3BucketName, getBackupFile(backupName, tenantTable, scanCount),
                    toFlush.byteInputStream(charset), objectMetadata)
            s3.putObject(putObjectReq)
        }
    }
}

data class TenantTableRow(val attributeMap: Map<String, AttributeValue>)

class MtSharedTableBackupManagerBuilder(val s3: AmazonS3, val s3BucketName: String, val tableSnapshotter: MtBackupTableSnapshotter) {
    fun build(sharedTableMtDynamo: MtAmazonDynamoDbBySharedTable): MtSharedTableBackupManager {
        return MtSharedTableBackupManager(s3, s3BucketName, sharedTableMtDynamo, tableSnapshotter)
    }
}