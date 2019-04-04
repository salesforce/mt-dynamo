package com.salesforce.dynamodbv2.mt.util;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DynamoDbCapacityTest {

    private final DynamoDbCapacity capacityTest = new DynamoDbCapacity();

    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";
    private CreateTableRequest request;

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
    }

    // Tests if createTableRequest.BillingMode is already set to PAY_PER_REQUEST, then billing mode shouldn't change
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
    void testNoChangeWithBillingModeAlreadyPayPerRequestForInputBillingModeNull() {
        request.withBillingMode(BillingMode.PAY_PER_REQUEST);
        DynamoDbCapacity.setBillingMode(request, null);
        assert (request.getBillingMode().equals(BillingMode.PAY_PER_REQUEST.toString()));
        assertNull(request.getProvisionedThroughput());
    }

    @Test
    void testNoChangeWithBillingModeAlreadyPayPerRequestForInputBillingModePayPerRequest() {
        request.withBillingMode(BillingMode.PAY_PER_REQUEST);
        DynamoDbCapacity.setBillingMode(request, BillingMode.PAY_PER_REQUEST);
        assert (request.getBillingMode().equals(BillingMode.PAY_PER_REQUEST.toString()));
        assertNull(request.getProvisionedThroughput());
    }

    @Test
    void testNoChangeWithBillingModeAlreadyPayPerRequestForInputBillingModeProvisioned() {
        request.withBillingMode(BillingMode.PAY_PER_REQUEST);
        DynamoDbCapacity.setBillingMode(request, BillingMode.PROVISIONED);
        assert (request.getBillingMode().equals(BillingMode.PAY_PER_REQUEST.toString()));
        assertNull(request.getProvisionedThroughput());
    }

    // Tests if createTableRequest.BillingMode is already set to PROVISIONED, then billing mode shouldn't change and
    // throughput should be set if supplied
    @Test
    void testNoChangeWithBillingModeAlreadyProvisionedForInputBillingModeNull() {
        request.withBillingMode(BillingMode.PROVISIONED);
        DynamoDbCapacity.setBillingMode(request, null);
        assert (request.getBillingMode().equals(BillingMode.PROVISIONED.toString()));
        assertProvisionedThroughputResults(request, 1L);
    }

    @Test
    void testNoChangeWithBillingModeAlreadyProvisionedForInputBillingModePayPerRequest() {
        request.withBillingMode(BillingMode.PROVISIONED);
        DynamoDbCapacity.setBillingMode(request, BillingMode.PAY_PER_REQUEST);
        assert (request.getBillingMode().equals(BillingMode.PROVISIONED.toString()));
        assertProvisionedThroughputResults(request, 1L);
    }

    @Test
    void testNoChangeWithBillingModeAlreadyProvisionedForInputBillingModeProvisioned() {
        request.withBillingMode(BillingMode.PROVISIONED);
        DynamoDbCapacity.setBillingMode(request, BillingMode.PROVISIONED);
        assert (request.getBillingMode().equals(BillingMode.PROVISIONED.toString()));
        assertProvisionedThroughputResults(request, 1L);
    }

    // Tests if createTableRequest.ProvisionedThroughput is already set then ProvisionedThroughtput doesn't change
    @Test
    void testNoChangeWithProvisionedThroughputAlreadySetForInputBillingModeNull() {
        request.withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
        DynamoDbCapacity.setBillingMode(request, null);
        assertProvisionedThroughputResults(request, 5L);
    }

    @Test
    void testNoChangeWithProvisionedThroughputAlreadySetForInputBillingModePayPerRequest() {
        request.withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
        DynamoDbCapacity.setBillingMode(request, BillingMode.PAY_PER_REQUEST);
        assertProvisionedThroughputResults(request, 5L);
    }

    @Test
    void testNoChangeWithProvisionedThroughputAlreadySetForInputBillingModeProvisioned() {
        request.withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
        DynamoDbCapacity.setBillingMode(request, BillingMode.PROVISIONED);
        assertProvisionedThroughputResults(request, 5L);
    }

    // Tests if createTableRequest is a fresh request (no billing mode/provisioned throughput set that the input values
    // are used to determine the proper billing mode/provisioned throughput
    @Test
    void testBillingModePayPerRequestForInputBillingModePayPerRequestAndProvisionedThroughputNull() {
        DynamoDbCapacity.setBillingMode(request, BillingMode.PAY_PER_REQUEST);
        assert (request.getBillingMode().equals(BillingMode.PAY_PER_REQUEST.toString()));
        assertNull(request.getProvisionedThroughput());
    }

    @Test
    void testBillingModeProvisionedForInputBillingModeNullAndProvisionedThroughputNull() {
        DynamoDbCapacity.setBillingMode(request, null);
        assert (request.getBillingMode().equals(BillingMode.PROVISIONED.toString()));
        assertProvisionedThroughputResults(request, 1L);
    }

    @Test
    void testBillingModeProvisionedForInputBillingModeProvisionedAndProvisionedThroughputNull() {
        DynamoDbCapacity.setBillingMode(request, BillingMode.PROVISIONED);
        assert (request.getBillingMode().equals(BillingMode.PROVISIONED.toString()));
        assertProvisionedThroughputResults(request, 1L);
    }
}

