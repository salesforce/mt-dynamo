package com.salesforce.dynamodbv2.mt.util;


import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

/**
 * A utility for working with ProvisionedThroughput in DynamoDB
 *
 */
public class DynamoDbCapacity {

    public enum CapacityType {
        READ("READ"),
        WRITE("WRITE");

        private String value;

        CapacityType(String value) {
            this.value = value;
        }

        public String toString() {
            return this.value;
        }
    }

    public Long getCapacity(ProvisionedThroughput throughput, CapacityType capacityType) {
        long capacity = 1L;
        if (throughput != null && capacityType.equals(CapacityType.READ)) {
            capacity = throughput.getReadCapacityUnits();
        } else if (throughput != null && capacityType.equals(CapacityType.WRITE)) {
            capacity = throughput.getWriteCapacityUnits();
        }
        return capacity;
    }
}
