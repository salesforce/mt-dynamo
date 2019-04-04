package com.salesforce.dynamodbv2.mt.util;

import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

/**
 * A utility for working with ProvisionedThroughput in DynamoDB.
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

        @Override
        public String toString() {
            return this.value;
        }
    }

    /**
     * Returns the capacity for a given ProvisionedThroughput type.
     * @param throughput - ProvisionedThrouhgput instance
     * @param capacityType - Type of ProvisionedThrouhgput
     * @return capacity set for given capacityType or default capacity if unset
     */
    public Long getCapacity(ProvisionedThroughput throughput, CapacityType capacityType) {
        long capacity = 1L;
        if (throughput != null && capacityType.equals(CapacityType.READ)) {
            capacity = throughput.getReadCapacityUnits();
        } else if (throughput != null && capacityType.equals(CapacityType.WRITE)) {
            capacity = throughput.getWriteCapacityUnits();
        }
        return capacity;
    }

    /**
     * Updates the createTableRequest with Billing Mode details (PAY_PER_REQUEST or PROVISIONED).
     * @param createTableRequest the table request {@code CreateTableRequest} instance
     * @param billingMode the desired billing mode
     *      precedence order: If createTableRequest.PAY_PER_REQUEST or createTableRequest.PROVISIONED, billing mode
     *      won't change.  Next if ProvisionedThroughput is already set on the createTableRequest.  Finally billingMode
     *      input parameter is used to determine billing mode. Null billingMode defaults to PROVISIONED.
     * */
    public static void setBillingMode(CreateTableRequest createTableRequest, BillingMode billingMode) {

        String billingModeFromRequest = createTableRequest.getBillingMode();

        // Only set Pay Per Request if ProvisionedThroughput is not already set on this request.
        if (billingMode != null && billingMode.equals(BillingMode.PAY_PER_REQUEST)
            && (billingModeFromRequest == null || !billingModeFromRequest.equals(BillingMode.PROVISIONED.toString()))
            && createTableRequest.getProvisionedThroughput() == null) {
            createTableRequest.withBillingMode(BillingMode.PAY_PER_REQUEST);
        } else if ((billingModeFromRequest == null || billingModeFromRequest.equals(BillingMode.PROVISIONED.toString()))
                && createTableRequest.getProvisionedThroughput() == null) {
            createTableRequest.withProvisionedThroughput(new ProvisionedThroughput(
                    1L, 1L));
            createTableRequest.withBillingMode(BillingMode.PROVISIONED);
        }
    }
}
