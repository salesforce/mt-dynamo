package com.salesforce.dynamodbv2.mt.util;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.*;

import com.amazonaws.services.dynamodbv2.model.*;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DynamoDbCapacityTest {

    DynamoDbCapacity capacityTest = new DynamoDbCapacity();

    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";

    private CreateTableRequest request, requestWithProvisionedThroughput;
    private static final String HASH_KEY_FIELD = "hk";
    private static final String RANGE_KEY_FIELD = "rk";

    void assertProvisionedThroughputResults(CreateTableRequest request, Long expectedProvisionedThroughput) {
        assertNotEquals(BillingMode.PAY_PER_REQUEST.toString(), request.getBillingMode());
        assertNotNull(request.getProvisionedThroughput());
        assert (request.getProvisionedThroughput().getReadCapacityUnits().equals(expectedProvisionedThroughput));
        assert (request.getProvisionedThroughput().getWriteCapacityUnits().equals(expectedProvisionedThroughput));
    }

    @BeforeEach
    void beforeEach() {
        request = new CreateTableRequest()
                .withTableName("testTable")
                .withKeySchema(new KeySchemaElement(ID_ATTR_NAME, HASH))
                .withAttributeDefinitions(
                        new AttributeDefinition(ID_ATTR_NAME, S),
                        new AttributeDefinition(INDEX_ID_ATTR_NAME, S));

        requestWithProvisionedThroughput = new CreateTableRequest()
                .withTableName("testTable2")
                .withKeySchema(new KeySchemaElement(ID_ATTR_NAME, HASH))
                .withAttributeDefinitions(
                        new AttributeDefinition(ID_ATTR_NAME, S),
                        new AttributeDefinition(INDEX_ID_ATTR_NAME, S))
                .withProvisionedThroughput(new ProvisionedThroughput(1L,1L));
    }

    @Test
    void testDefaultCapacitySetForNullThroughput() {
        long actual = capacityTest.getCapacity(null, DynamoDbCapacity.CapacityType.READ);
        assertEquals(1L, actual);

        actual = capacityTest.getCapacity(null, DynamoDbCapacity.CapacityType.WRITE);
        assertEquals(1L, actual);
    }

    @Test
    void testCapacitySetForNonNullThroughput() {
        ProvisionedThroughput throughput = new ProvisionedThroughput(5L, 10L);
        long actual = capacityTest.getCapacity(throughput, DynamoDbCapacity.CapacityType.READ);
        assertEquals(5L, actual);

        actual = capacityTest.getCapacity(throughput, DynamoDbCapacity.CapacityType.WRITE);
        assertEquals(10L, actual);
    }

    @Test
    void testBillingModeSetToPayPerRequest() {
        capacityTest.setBillingMode(request, BillingMode.PAY_PER_REQUEST, null);
        assert (request.getBillingMode().equals(BillingMode.PAY_PER_REQUEST.toString()));
        assertNull(request.getProvisionedThroughput());
    }

    @Test
    void testBillingModeSetToPayPerRequestIfRequestAlreadySetToPayPerRequest() {
        request.withBillingMode(BillingMode.PAY_PER_REQUEST);
        capacityTest.setBillingMode(request, BillingMode.PAY_PER_REQUEST, null);
        assert (request.getBillingMode().equals(BillingMode.PAY_PER_REQUEST.toString()));
        assertNull(request.getProvisionedThroughput());
    }

    @Test
    void testBillingModeSetToPayPerRequestIfInputBillingModeNull() {
        request.withBillingMode(BillingMode.PAY_PER_REQUEST);
        capacityTest.setBillingMode(request, null, null);
        assert (request.getBillingMode().equals(BillingMode.PAY_PER_REQUEST.toString()));
        assertNull(request.getProvisionedThroughput());
    }

    @Test
    void testBillingModeSetToProvisionedIfNull() {
        capacityTest.setBillingMode(request, null, 1L);
        assertProvisionedThroughputResults(request, 1L);
    }

    @Test
    void testBillingModeSetToProvisionedIfProvisionedThroughputIsNull() {
        capacityTest.setBillingMode(request, BillingMode.PROVISIONED, null);
        assertProvisionedThroughputResults(request, 1L);
    }

    @Test
    void testBillingModeSetToProvisionedIfProvisionedThroughputIsSet() {
        capacityTest.setBillingMode(request, BillingMode.PROVISIONED, 5L);
        assertProvisionedThroughputResults(request, 5L);
    }

    @Test
    void testRequestWithProvisionedThroughputDoesNotHavePPRSet() {
        capacityTest.setBillingMode(requestWithProvisionedThroughput, null, 1L);
        assertProvisionedThroughputResults(requestWithProvisionedThroughput, 1L);
    }

    // TODO add some additional tests
}

