package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import org.apache.commons.lang.NotImplementedException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class MTAmazonDynamoDBStreamsTest {

    /**
     * Verifies that getting a streams client for a non-multi-tenant dynamo instance returns the pass-through instance.
     */
    @Test
    void testCreateFromDynamoNotMultitenant() {
        AmazonDynamoDBStreams expected = mock(AmazonDynamoDBStreams.class);

        AmazonDynamoDBStreams actual =
                MTAmazonDynamoDBStreams.createFromDynamo(mock(AmazonDynamoDB.class), expected);

        assertTrue(actual instanceof MTAmazonDynamoDBStreamsPassthrough);
    }

    /**
     * Verifies that getting a streams client for an instance of {@link MTAmazonDynamoDBByTable} yields a pass-through
     * instance of {@link MTAmazonDynamoDBStreams}.
     */
    @Test
    void testCreateFromDynamoByTable() {
        AmazonDynamoDBStreams actual = MTAmazonDynamoDBStreams
                .createFromDynamo(mock(MTAmazonDynamoDBByTable.class), mock(AmazonDynamoDBStreams.class));

        assertTrue(actual instanceof MTAmazonDynamoDBStreamsPassthrough,
                "Expected an instance of MTAmazonDynamoDBStreamsPassthrough");
    }

    @Test
    void testCreateFromDynamoByAccount() {
        // TODO Not implemented yet
        assertThrows(NotImplementedException.class, () -> MTAmazonDynamoDBStreams
                .createFromDynamo(mock(MTAmazonDynamoDBByAccount.class), mock(AmazonDynamoDBStreams.class)));
    }

    @Test
    void testCreateFromDynamoByIndex() {
        // TODO Not implemented yet
        assertThrows(NotImplementedException.class, () -> MTAmazonDynamoDBStreams
                .createFromDynamo(mock(MTAmazonDynamoDBByIndex.class), mock(AmazonDynamoDBStreams.class)));
    }
}