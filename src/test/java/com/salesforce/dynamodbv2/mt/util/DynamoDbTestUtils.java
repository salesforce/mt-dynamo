package com.salesforce.dynamodbv2.mt.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import java.util.List;

/**
 * Utility functions for tests that do DynamoDB table creation/deletion.
 */
public class DynamoDbTestUtils {


    /**
     * For a given table, check that the table has the expected properties set for BillingMode.PAY_PER_REQUEST.
     * @param tableName the table name
     * @param dynamoDbInstance the AmazonDynamoDB instance
     */
    public static void assertPayPerRequestIsSet(String tableName,  AmazonDynamoDB dynamoDbInstance) {
        TableDescription tableDescription = dynamoDbInstance.describeTable(tableName).getTable();

        assertEquals(BillingMode.PAY_PER_REQUEST.toString(), tableDescription.getBillingModeSummary().getBillingMode());
        assert (tableDescription.getProvisionedThroughput().getReadCapacityUnits().equals(0L));
        assert (tableDescription.getProvisionedThroughput().getWriteCapacityUnits().equals(0L));
    }

    /**
     * For a given table, check that the table has the expected properties set for BillingMode.PROVISIONED.
     * @param tableName the table name
     * @param dynamoDbInstance the AmazonDynamoDB instance
     * @param expectedThroughput the expected ProvisionedThroughput value
     */
    public static void assertProvisionedIsSet(String tableName,  AmazonDynamoDB dynamoDbInstance,
                                              Long expectedThroughput) {
        TableDescription tableDescription = dynamoDbInstance.describeTable(tableName).getTable();

        assertEquals(BillingMode.PROVISIONED.toString(),tableDescription.getBillingModeSummary().getBillingMode());

        assertNotNull(tableDescription.getProvisionedThroughput());
        assert (tableDescription.getProvisionedThroughput().getReadCapacityUnits().equals(expectedThroughput));
        assert (tableDescription.getProvisionedThroughput().getWriteCapacityUnits().equals(expectedThroughput));
    }

    /**
     * For a list of tables, check that the table(s) have the expected properties set for BillingMode.PROVISIONED.
     * @param testTables list of tables to verify
     * @param dynamoDbInstance the AmazonDynamoDB instance
     * @param expectedThroughput the expected ProvisionedThroughput value
     */
    public static void assertProvisionedIsSetForSetOfTables(List<String> testTables, AmazonDynamoDB dynamoDbInstance,
                                                      Long expectedThroughput) {
        for (String table: testTables) {
            assertProvisionedIsSet(table, dynamoDbInstance, expectedThroughput);
        }
    }

    public static String getTableNameWithPrefix(String tablePrefix, String tableName, String delimeter) {
        return tablePrefix + delimeter + tableName;
    }

    public static String getTimestampTableName() {
        return String.valueOf(System.currentTimeMillis());
    }
}
