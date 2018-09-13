package com.salesforce.dynamodbv2.mt.mappers;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbStreamsBySharedTable;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.Test;

class MtAmazonDynamoDbStreamsTest {

    /**
     * Verifies that getting a streams client for a non-multi-tenant dynamo instance returns the pass-through instance.
     */
    @Test
    void testCreateFromDynamoNotMultitenant() {
        AmazonDynamoDBStreams expected = mock(AmazonDynamoDBStreams.class);

        AmazonDynamoDBStreams actual =
            MtAmazonDynamoDbStreams.createFromDynamo(mock(AmazonDynamoDB.class), expected);

        assertTrue(actual instanceof MtAmazonDynamoDbStreamsPassthrough);
    }

    /**
     * Verifies that getting a streams client for an instance of {@link MtAmazonDynamoDbByTable} yields a pass-through
     * instance of {@link MtAmazonDynamoDbStreams}.
     */
    @Test
    void testCreateFromDynamoByTable() {
        AmazonDynamoDBStreams actual = MtAmazonDynamoDbStreams
            .createFromDynamo(mock(MtAmazonDynamoDbByTable.class), mock(AmazonDynamoDBStreams.class));

        assertTrue(actual instanceof MtAmazonDynamoDbStreamsPassthrough,
            "Expected an instance of MtAmazonDynamoDbStreamsPassthrough");
    }

    @Test
    void testCreateFromDynamoByAccount() {
        // TODO Not implemented yet
        assertThrows(NotImplementedException.class, () -> MtAmazonDynamoDbStreams
            .createFromDynamo(mock(MtAmazonDynamoDbByAccount.class), mock(AmazonDynamoDBStreams.class)));
    }

    @Test
    void testCreateFromDynamoByIndex() {
        // TODO Not implemented yet
        AmazonDynamoDBStreams actual = MtAmazonDynamoDbStreams
                .createFromDynamo(mock(MtAmazonDynamoDbBySharedTable.class), mock(AmazonDynamoDBStreams.class));

        assertTrue(actual instanceof MtAmazonDynamoDbStreamsBySharedTable,
                "Expected an instance of MtAmazonDynamoDbStreamsBySharedTable");
    }
}