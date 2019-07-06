/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.sharedtable.impl

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.DeleteObjectRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3Object
import com.google.common.collect.Lists
import com.google.common.collect.Multimap
import com.google.common.collect.MultimapBuilder
import com.google.common.collect.Sets
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbBase
import com.salesforce.dynamodbv2.mt.sharedtable.CreateMtBackupRequest
import com.salesforce.dynamodbv2.mt.sharedtable.MtBackupManager
import com.salesforce.dynamodbv2.mt.sharedtable.MtBackupMetadata
import com.salesforce.dynamodbv2.mt.sharedtable.RestoreMtBackupRequest
import com.salesforce.dynamodbv2.mt.sharedtable.Status
import com.salesforce.dynamodbv2.mt.sharedtable.TenantRestoreMetadata
import com.salesforce.dynamodbv2.mt.sharedtable.TenantTableBackupMetadata
import java.nio.charset.Charset
import java.util.stream.Collectors

/**
 * Take an mt-dynamo set of tables, and using relatively naive scans across each table, build a set of backups
 * per tenant.
 */
open class MtBackupManagerImpl(region: String, val s3BucketName: String) : MtBackupManager {

    val s3: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(region).build()
    val gson: Gson = Gson()
    val charset = Charset.forName("utf-8")

    override fun getTenantTableBackup(id: String): TenantTableBackupMetadata {
        TODO("not implemented")
    }

    override fun restoreTenantTableBackup(
        restoreMtBackupRequest: RestoreMtBackupRequest,
        mtDynamo: MtAmazonDynamoDbBase,
        mtContext: MtAmazonDynamoDbContextProvider
    ): TenantRestoreMetadata {

        val backupFileKeys = getBackupFileKeys(restoreMtBackupRequest.backupId, restoreMtBackupRequest.tenantTableBackup)
        for (fileName in backupFileKeys) {
            val backupFile: S3Object = s3.getObject(s3BucketName, fileName)
            val rowsToInsert: List<TenantTableRow> = gson.fromJson(backupFile.objectContent.bufferedReader(),
                    object : TypeToken<List<TenantTableRow>>() { }.type)
            for (row in rowsToInsert) {
                mtContext.withContext(restoreMtBackupRequest.newTenantTable.tenantName) {
                    mtDynamo.putItem(restoreMtBackupRequest.newTenantTable.virtualTable, row.attributeMap)
                }
            }
        }
        return TenantRestoreMetadata(restoreMtBackupRequest.backupId,
                Status.COMPLETE,
                restoreMtBackupRequest.newTenantTable.tenantName,
                restoreMtBackupRequest.newTenantTable.virtualTable)
    }

    private fun getBackupFileKeys(backupId: String, tenantTable: TenantTable): Set<String> {
        val ret = Sets.newHashSet<String>()
        var continuationToken: String? = null
        do {
            val listBucketResult: ListObjectsV2Result = s3.listObjectsV2(
                    ListObjectsV2Request()
                            .withBucketName(s3BucketName)
                            .withContinuationToken(continuationToken)
                            .withPrefix("$backupDir/$backupId/${tenantTable.tenantName}/${tenantTable.virtualTable}"))
            continuationToken = listBucketResult.continuationToken
            for (o in listBucketResult.objectSummaries) {
                ret.add(o.key)
            }
        } while (listBucketResult.isTruncated)
        return ret
    }

    override fun listMtBackups(): List<MtBackupMetadata> {
        val ret = Lists.newArrayList<MtBackupMetadata>()
        var continuationToken: String? = null
        do {
            val listBucketResult: ListObjectsV2Result = s3.listObjectsV2(
                    ListObjectsV2Request()
                            .withDelimiter("/")
                            .withBucketName(s3BucketName)
                            .withContinuationToken(continuationToken)
                            .withPrefix(backupMetadataDir + "/"))
            continuationToken = listBucketResult.continuationToken
            for (o in listBucketResult.objectSummaries) {
                val backup = getBackup(getBackupIdFromKey(o.key))
                ret.add(backup)
            }
        } while (listBucketResult.isTruncated)
        return ret
    }

    override fun getBackup(id: String): MtBackupMetadata? {
        try {
            val backupFile: S3Object = s3.getObject(s3BucketName, getBackupMetadataFile(id))
            return gson.fromJson<MtBackupMetadata>(backupFile.objectContent.bufferedReader(), MtBackupMetadata::class.java)
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
        println("Deleted $deleteCount backup files for $id")
        val ret = getBackup(id)
        s3.deleteObject(DeleteObjectRequest(s3BucketName, getBackupMetadataFile(id)))
        return ret
    }

    /**
     * Go through the shared data table and dump full row dumps into S3, segregated by tenant-table.
     *
     * Format on S3 looks like:
     *
     * ${backupBucket}/metadata.json <- Overview of full backup and what is contained
     * backupBucket/${tenant1}/${virtualTable1}-${scanCount}.json
     * backupBucket/${tenant1}/${virtualTable2}-${scanCount}.json
     * ...
     * backupBucket/${tenantN}/${virtualTableN}-${N}}.json
     *
     * High level, the multitenant shared table is being scanned, and with each page of results, we write a file per
     * tenant-table onto S3 organized such that restoring a single tenant-table is a simple S3 directory listFiles
     * operation providing full row dumps to insert|restore back into dynamo under a different tenant-table space.
     */
    override fun createMtBackup(createMtBackupRequest: CreateMtBackupRequest, mtDynamo: MtAmazonDynamoDbBase): MtBackupMetadata {
        // write out actual backup data
        val backupMetadata = createBackupData(createMtBackupRequest, mtDynamo)
        commitBackupMetadata(backupMetadata)
        return backupMetadata
    }

    protected fun commitBackupMetadata(backupMetadata: MtBackupMetadata) {
        // write out metadata of what was backed up
        val backupMetadataBytes = gson.toJson(backupMetadata).toByteArray(charset)
        val objectMetadata = ObjectMetadata()
        objectMetadata.contentLength = backupMetadataBytes.size.toLong()
        objectMetadata.contentType = "application/json"
        val putObjectReq = PutObjectRequest(s3BucketName, getBackupMetadataFile(backupMetadata.mtBackupId),
                gson.toJson(backupMetadata).byteInputStream(charset), objectMetadata)
        s3.putObject(putObjectReq)
    }

    protected open fun createBackupData(createMtBackupRequest: CreateMtBackupRequest, mtDynamo: MtAmazonDynamoDbBase): MtBackupMetadata {
        val sharedTable = createMtBackupRequest.sharedTableName
        var lastRow: TenantTableRow? = null
        val tenantTables = Sets.newHashSet<TenantTable>()
        var numPasses = 0
        do {
            val scanRequest: ScanRequest = ScanRequest(sharedTable)
                    .withExclusiveStartKey(lastRow?.attributeMap)
            val scanResult = mtDynamo.scan(scanRequest)
            if (scanResult.lastEvaluatedKey != null) {
                lastRow = TenantTableRow(scanResult.lastEvaluatedKey)
            }

            val rowsPerTenant = MultimapBuilder.ListMultimapBuilder
                    .linkedHashKeys().hashSetValues().build<TenantTable, TenantTableRow>()
            for (i in 0..(scanResult.items.size - 1)) {
                val row = scanResult.items[i]
                val tenant = row.get(MtAmazonDynamoDbBase.TENANT_KEY)!!.s
                val virtualTable = row.remove(MtAmazonDynamoDbBase.VIRTUAL_TABLE_KEY)!!.s
                rowsPerTenant.put(TenantTable(tenantName = tenant, virtualTable = virtualTable),
                        TenantTableRow(scanResult.items[i]))
            }

            println("Flushed ${rowsPerTenant.values().size} rows for " +
                    "${rowsPerTenant.keySet().size} tenants.")
            tenantTables.addAll(rowsPerTenant.keySet())
            flushScanResult(createMtBackupRequest.backupId, numPasses, rowsPerTenant)
        } while (scanResult.count > 0 && ++numPasses > 1)
        val tenantTableBackupMetadatas = tenantTables
                .stream()
                .map { TenantTableBackupMetadata(createMtBackupRequest.backupId,
                        Status.COMPLETE, it.tenantName, it.virtualTable,
                        getBackupFileKeys(createMtBackupRequest.backupId, it)) }
                .collect(Collectors.toSet())
        return MtBackupMetadata(createMtBackupRequest.backupId, Status.COMPLETE,
                tenantTableBackupMetadatas)
    }

    private val backupMetadataDir = "backupMetadata"
    private val backupDir = "backups"

    private fun getBackupMetadataFile(backupId: String) = "$backupMetadataDir/$backupId-metadata.json"
    private fun getBackupFile(backupId: String, tenantTable: TenantTable, scanCount: Int) =
            "$backupDir/$backupId/${tenantTable.tenantName}/${tenantTable.virtualTable}-$scanCount.json"

    private fun getBackupIdFromKey(backupMetadataKey: String) = backupMetadataKey.split("/")[1].split("-metadata")[0]

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

data class TenantTable(val virtualTable: String, val tenantName: String)
data class TenantTableRow(val attributeMap: Map<String, AttributeValue>)