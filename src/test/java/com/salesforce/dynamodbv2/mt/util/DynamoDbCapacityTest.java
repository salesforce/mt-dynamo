package com.salesforce.dynamodbv2.mt.util;

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DynamoDbCapacityTest {

    DynamoDbCapacity capacityTest = new DynamoDbCapacity();

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
}