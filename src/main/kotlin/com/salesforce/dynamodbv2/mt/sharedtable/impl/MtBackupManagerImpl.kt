package com.salesforce.dynamodbv2.mt.sharedtable.impl

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3Object
import com.google.common.collect.ImmutableSet
import com.google.gson.Gson
import com.salesforce.dynamodbv2.mt.sharedtable.*
import java.nio.charset.Charset


class MtBackupManagerImpl(region: String, val s3BucketName: String) : MtBackupManager {
    val s3 : AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(region).build()
    val gson: Gson = Gson()
    val charset = Charset.forName("utf-8")

    override fun getTenantTableBackup(id: String): TenantTableBackupMetadata {
        TODO("not implemented")
    }

    override fun restoreTenantTableBackup(id: String, newTenantId: String, newTenantTableName: String): TenantRestoreMetadata {
        TODO("not implemented")
    }

    override fun listMtBackups(): List<MtBackupMetadata> {
        TODO("not implemented")
    }

    override fun getBackup(id: String): MtBackupMetadata {
        val backupFile : S3Object = s3.getObject(s3BucketName, getBackupFile(id))
        return gson.fromJson<MtBackupMetadata>(backupFile.objectContent.bufferedReader(), MtBackupMetadata::class.java)
    }

    override fun terminateBackup(id: String): MtBackupMetadata {
        TODO("not implemented")
    }

    /**
     * For now, just create a metadata marker indicating a backup has been taken.
     *
     * The next step is scanning the shared table, and writing backup files per tenant table.
     */
    override fun createMtBackup(id: String): MtBackupMetadata {
        val backupMetadata = MtBackupMetadata(id, Status.COMPLETE, ImmutableSet.of())
        val backupMetadataBytes = gson.toJson(backupMetadata).toByteArray(charset)
        val objectMetadata = ObjectMetadata()
        objectMetadata.contentLength = backupMetadataBytes.size.toLong()
        objectMetadata.contentType = "application/json"
        val putObjectReq = PutObjectRequest(s3BucketName, getBackupFile(id), gson.toJson(backupMetadata).byteInputStream(charset), objectMetadata)
        s3.putObject(putObjectReq)
        return backupMetadata
    }

    private fun getBackupFile(id: String) = id + "/metadata.json"
}