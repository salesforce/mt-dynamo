package com.salesforce.dynamodbv2.mt.mappers;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbStreamsBySharedTable;

import org.junit.jupiter.api.Test;

public class MtAmazonDynamoDbStreamsTest {

    /**
     * Verifies that getting a streams client for a non-multitenant dynamo instance throws an exception.
     */
    @Test
    void testCreateFromDynamoNotMultitenant() {
        assertThrows(IllegalArgumentException.class, () -> MtAmazonDynamoDbStreams
            .createFromDynamo(mock(AmazonDynamoDB.class), mock(AmazonDynamoDBStreams.class)));
    }

    /**
     * Verifies that getting a streams client for an instance of {@link MtAmazonDynamoDbByTable} yields a pass-through
     * instance of {@link MtAmazonDynamoDbStreams}.
     */
    @Test
    void testCreateFromDynamoByTable() {
        AmazonDynamoDBStreams actual = MtAmazonDynamoDbStreams
            .createFromDynamo(mock(MtAmazonDynamoDbByTable.class), mock(AmazonDynamoDBStreams.class));

        assertTrue(actual instanceof MtAmazonDynamoDbStreamsByTable,
            "Expected an instance of MtAmazonDynamoDbStreamsByTable");
    }

    @Test
    void testCreateFromDynamoByAccount() {
        // TODO Not implemented yet
        assertThrows(UnsupportedOperationException.class, () -> MtAmazonDynamoDbStreams
            .createFromDynamo(mock(MtAmazonDynamoDbByAccount.class), mock(AmazonDynamoDBStreams.class)));
    }

    @Test
    void testCreateFromDynamoByIndex() {
        AmazonDynamoDBStreams actual = MtAmazonDynamoDbStreams
            .createFromDynamo(mock(MtAmazonDynamoDbBySharedTable.class), mock(AmazonDynamoDBStreams.class));

        assertTrue(actual instanceof MtAmazonDynamoDbStreamsBySharedTable,
            "Expected an instance of MtAmazonDynamoDbStreamsBySharedTable");
    }

}