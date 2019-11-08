package com.salesforce.dynamodbv2.mt.backups

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

/**
 * Integration test of code leveraging DynamoDB's on-demand backups to facilitate taking tenant-table level backups.
 * Local Dynamo does not support taking on-demand backups, so this test lives as a proper integration test against
 * remote hosted dynamodb. Beware: the test creates real tables with real costs, and consequently takes a long time
 * to run.
 */
internal class MtBackupTableSnapshotterIt {

    @Test
    fun testTableSnapshot() {
        Preconditions.checkArgument(System.getenv("AWS_ACCESS_KEY_ID") != null)
        Preconditions.checkArgument(System.getenv("AWS_SECRET_ACCESS_KEY") != null)

        // dynamo details
        val region = Regions.US_EAST_1
        val remoteDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(EnvironmentVariableCredentialsProvider())
                .withRegion(region).build()

        // dummy table details
        val dummyTable = "test-table-snapshot"
        val dummyPk = "primary-key"
        val dummyCreateTable = CreateTableRequest(dummyTable, ImmutableList.of(KeySchemaElement(dummyPk, KeyType.HASH)))
                .withAttributeDefinitions(ImmutableSet.of(AttributeDefinition(dummyPk, ScalarAttributeType.S)))
                .withProvisionedThroughput(ProvisionedThroughput(10, 10))

        if (!tableExists(dummyTable, remoteDynamoDB)) {
            val dummyCreateTableResult = remoteDynamoDB.createTable(dummyCreateTable)
            assertNotNull(dummyCreateTableResult)
            assertEquals(dummyTable, dummyCreateTableResult.tableDescription.tableName)
        }

        // wait for table to be active
        var dummyTableStatus: DescribeTableResult
        do {
            dummyTableStatus = remoteDynamoDB.describeTable(DescribeTableRequest(dummyTable))
            if (dummyTableStatus.table.tableStatus == "CREATING") {
                Thread.sleep(1000L)
            }
        } while (dummyTableStatus.table.tableStatus == "CREATING")

        // insert dummy data
        assertEquals("ACTIVE", dummyTableStatus.table.tableStatus)
        val dummyColumn = "value"
        val dummyRow = ImmutableMap.of(
                dummyPk, AttributeValue().withS("pk1"),
                dummyColumn, AttributeValue().withS("value1"))
        remoteDynamoDB.putItem(PutItemRequest(dummyTable,
                dummyRow))

        // issue snapshot request
        val backupId = "backup-test"
        val dummyTableSnapshot = "test-table-snapshot-copy"
        val snapshotRequest = SnapshotRequest(backupId, dummyTable, dummyTableSnapshot, remoteDynamoDB)
        val snapshotResult = MtBackupTableSnapshotter().snapshotTableToTarget(snapshotRequest)
        try {
            assertNotNull(snapshotResult)
            assertEquals(dummyTableSnapshot, snapshotResult.tempSnapshotTable)

            //validate dummy data on snapshotted table
            val getItemResult = remoteDynamoDB.getItem(GetItemRequest(dummyTableSnapshot, ImmutableMap.of(
                    dummyPk, AttributeValue().withS("pk1"))))
            assertNotNull(getItemResult)
            assertEquals("value1", getItemResult.item[dummyColumn]?.s)
        } finally {
            remoteDynamoDB.deleteTable(DeleteTableRequest(dummyTable))
            MtBackupTableSnapshotter().cleanup(snapshotResult, remoteDynamoDB)
        }
    }

    private fun tableExists(tableName: String, remoteAmazonDynamoDB: AmazonDynamoDB): Boolean {
        return try {
            remoteAmazonDynamoDB.describeTable(tableName).table.tableName == tableName
        } catch (e: ResourceNotFoundException) {
            false
        }
    }
}