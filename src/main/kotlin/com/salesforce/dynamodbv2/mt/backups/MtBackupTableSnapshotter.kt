/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.dynamodbv2.mt.backups

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.BackupStatus
import com.amazonaws.services.dynamodbv2.model.ContinuousBackupsUnavailableException
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest
import com.amazonaws.services.dynamodbv2.model.CreateBackupResult
import com.amazonaws.services.dynamodbv2.model.DeleteBackupRequest
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeBackupRequest
import com.amazonaws.services.dynamodbv2.model.DescribeBackupResult
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupRequest
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder
import org.slf4j.LoggerFactory
import java.util.stream.Collectors

/**
 *
 * Utility to take correct backups of live production data with minimal risk.
 *
 * Instead of trying to operate against live tables taking traffic from production, backups operate against snapshotted
 * tables, which are scanned through at the backup's own pace. These tables are snapshotted using DynamoDB's own
 * on-demand backups, which are used to generate single point-in-time snapshots of live data. This protects against any
 * interleaved writes customers may drive against a live table (note that generating S3 tenant-table backups can be a
 * time-consuming operation). Additionally, it avoids competing with customer driven IOPs against prod dynamo tables by
 * getting a quiesced table from which the backup generator can exclusively consume all IOPs available to generate the
 * backup either as fast or as slow as needed.
 */
open class MtBackupTableSnapshotter {

    private val logger = LoggerFactory.getLogger(MtBackupTableSnapshotter::class.java)
    /**
     * Take a dynamo table and fire consecutive backup and restore requests to snapshot the table to a new table-space
     * location. This is a time consuming operation and should be done asynchronously (and potentially
     * resumed if interrupted).
     */
    open fun snapshotTableToTarget(snapshotRequest: SnapshotRequest): SnapshotResult {
        val startTime = System.currentTimeMillis()
        logger.info("Snapshot of ${snapshotRequest.sourceTableName} to ${snapshotRequest.targetTableName} beginning")
        // generate backup of table
        var backupResult: CreateBackupResult? = null
        do {
            try {
                backupResult = snapshotRequest.amazonDynamoDb.createBackup(CreateBackupRequest()
                        .withBackupName(snapshotRequest.targetTableName)
                        .withTableName(snapshotRequest.sourceTableName))
            } catch (e: ContinuousBackupsUnavailableException) {
                // wait for backups to be ready on this table, if not already
                Thread.sleep(1000L)
            }
        } while (backupResult == null)

        // wait for backup to be available
        var backupStatus: DescribeBackupResult
        do {
            backupStatus = snapshotRequest.amazonDynamoDb.describeBackup(
                    DescribeBackupRequest().withBackupArn(backupResult.backupDetails.backupArn))
            if (!backupStatus.backupDescription.backupDetails.backupStatus.equals(BackupStatus.CREATING)) {
                Thread.sleep(1000L)
            }
        } while (backupStatus.backupDescription.backupDetails.backupStatus.equals(BackupStatus.CREATING))
        if (backupStatus.backupDescription.backupDetails.backupStatus.equals(BackupStatus.DELETED)) {
            throw MtBackupException("Error while snapshotting ${snapshotRequest.sourceTableName}, " +
                    "snapshot backup marked DELETED")
        }

        logger.info("Snapshotting ${snapshotRequest.sourceTableName} to ${snapshotRequest.targetTableName}, " +
                "on-demand backup taken in ${System.currentTimeMillis() - startTime} ms")
        // restore table to new target
        val restoreResult = snapshotRequest.amazonDynamoDb.restoreTableFromBackup(RestoreTableFromBackupRequest()
                .withBackupArn(backupResult.backupDetails.backupArn)
                .withTargetTableName(snapshotRequest.targetTableName))
        if (!restoreResult.tableDescription.restoreSummary.restoreInProgress) {
            throw MtBackupException("Unexpected restore status " + restoreResult.tableDescription)
        }

        waitForActiveTable(snapshotRequest.targetTableName, snapshotRequest.amazonDynamoDb)
        logger.info("Finished snapshotting ${snapshotRequest.sourceTableName} to ${snapshotRequest.targetTableName}, " +
                "table restored and ready for use in ${System.currentTimeMillis() - startTime} ms")
        // wait for restore to complete
        return SnapshotResult(backupResult.backupDetails.backupArn,
                snapshotRequest.targetTableName,
                System.currentTimeMillis() - startTime)
    }

    open fun cleanup(snapshotResult: SnapshotResult, amazonDynamoDb: AmazonDynamoDB) {
        amazonDynamoDb.deleteBackup(DeleteBackupRequest().withBackupArn(snapshotResult.backupArn))
        amazonDynamoDb.deleteTable(DeleteTableRequest().withTableName(snapshotResult.tempSnapshotTable))
    }

    private fun waitForActiveTable(tableName: String, remoteDynamoDB: AmazonDynamoDB) {
        // wait for table to be active
        var tableStatus: DescribeTableResult
        do {
            tableStatus = remoteDynamoDB.describeTable(DescribeTableRequest(tableName))
            if (tableStatus.table.tableStatus.equals("CREATING")) {
                Thread.sleep(1000L)
            }
        } while (tableStatus.table.tableStatus.equals("CREATING"))
    }
}

/**
 * A mock table snapshotter that relies on naively scanning and copying table versus using on-demand backup features
 * unavailable on local dynamo.
 *
 * This should not be used in production environments, as it is not nearly as performant, nor does it guard against
 * interleaved writes during the backup, but is exposed to allow clients use a backup call that is actually performant.
 *
 * It is exposed in non-test code for clients test code to pull in when operating against local dynamo or dynalite.
 */
class MtScanningSnapshotter : MtBackupTableSnapshotter() {
    override fun snapshotTableToTarget(snapshotRequest: SnapshotRequest): SnapshotResult {
        val startTime = System.currentTimeMillis()
        val sourceTableDescription: TableDescription = snapshotRequest.amazonDynamoDb
                .describeTable(snapshotRequest.sourceTableName).table
        val createTableBuilder = CreateTableRequestBuilder.builder()
                .withTableName(snapshotRequest.targetTableName)
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

data class SnapshotRequest(
    val mtBackupName: String,
    val sourceTableName: String,
    val targetTableName: String,
    val amazonDynamoDb: AmazonDynamoDB,
    val targetTableProvisionedThroughput: ProvisionedThroughput =
            ProvisionedThroughput(10, 10)
)

data class SnapshotResult(
    val backupArn: String,
    val tempSnapshotTable: String,
    val snapshotPrepareTime: Long
)