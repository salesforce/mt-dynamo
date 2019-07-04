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
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3Object
import com.google.common.collect.Multimap
import com.google.common.collect.MultimapBuilder
import com.google.common.collect.Sets
import com.google.gson.Gson
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtScanResult
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

class MtBackupManagerImpl(region: String, val s3BucketName: String) : MtBackupManager {

    val s3: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(region).build()
    val gson: Gson = Gson()
    val charset = Charset.forName("utf-8")

    override fun getTenantTableBackup(id: String): TenantTableBackupMetadata {
        TODO("not implemented")
    }

    override fun restoreTenantTableBackup(restoreMtBackupRequest: RestoreMtBackupRequest): TenantRestoreMetadata {
        TODO("not implemented")
    }

    override fun listMtBackups(): List<MtBackupMetadata> {
        TODO("not implemented")
    }

    override fun getBackup(id: String): MtBackupMetadata {
        val backupFile: S3Object = s3.getObject(s3BucketName, getBackupMetadataFile(id))
        return gson.fromJson<MtBackupMetadata>(backupFile.objectContent.bufferedReader(), MtBackupMetadata::class.java)
    }

    override fun terminateBackup(id: String): MtBackupMetadata {
        TODO("not implemented")
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
        val sharedTable = createMtBackupRequest.sharedTableName
        var lastRow: TenantTableRow? = null
        val tenantTables = Sets.newHashSet<TenantTable>()
        var numPasses = 0
        do {
            val scanRequest: ScanRequest = ScanRequest(sharedTable)
                    .withExclusiveStartKey(lastRow?.attributeMap)
            val scanResult: MtScanResult = mtDynamo.scan(scanRequest) as MtScanResult
            if (scanResult.lastEvaluatedKey != null) {
                lastRow = TenantTableRow(scanResult.lastEvaluatedKey)
            }

            val rowsPerTenant = MultimapBuilder.ListMultimapBuilder
                    .linkedHashKeys().hashSetValues().build<TenantTable, TenantTableRow>()
            for (i in 0..(scanResult.items.size - 1)) {
                rowsPerTenant.put(TenantTable(scanResult.virtualTables[i], scanResult.tenants[i]),
                        TenantTableRow(scanResult.items[i]))
            }

            println("Flushed ${rowsPerTenant.values().size} rows for " +
                    "${rowsPerTenant.keySet().size} tenants.")
            tenantTables.addAll(rowsPerTenant.keySet())
            flushScanResult(createMtBackupRequest.backupId, numPasses, rowsPerTenant)
        } while (scanResult.count > 0 && ++numPasses > 1)
        // write metadata
        val tenantTableBackupMetadatas = tenantTables
                .stream()
                .map { TenantTableBackupMetadata(createMtBackupRequest.backupId,
                        Status.COMPLETE, it.tenantName, it.virtualTable) }
                .collect(Collectors.toSet())
        val backupMetadata = MtBackupMetadata(createMtBackupRequest.backupId, Status.COMPLETE,
                tenantTableBackupMetadatas)
        val backupMetadataBytes = gson.toJson(backupMetadata).toByteArray(charset)
        val objectMetadata = ObjectMetadata()
        objectMetadata.contentLength = backupMetadataBytes.size.toLong()
        objectMetadata.contentType = "application/json"
        val putObjectReq = PutObjectRequest(s3BucketName, getBackupMetadataFile(createMtBackupRequest.backupId),
                gson.toJson(backupMetadata).byteInputStream(charset), objectMetadata)
        s3.putObject(putObjectReq)
        return backupMetadata
    }

    private fun getBackupMetadataFile(backupId: String) = backupId + "/metadata.json"
    private fun getBackupFile(backupId: String, tenantTable: TenantTable, scanCount: Int) =
            "$backupId/${tenantTable.tenantName}/${tenantTable.virtualTable}-$scanCount.json"

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