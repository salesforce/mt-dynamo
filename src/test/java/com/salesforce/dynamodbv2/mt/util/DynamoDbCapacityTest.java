package com.salesforce.dynamodbv2.mt.util;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DynamoDbCapacityTest {

    private final DynamoDbCapacity capacityTest = new DynamoDbCapacity();

    private static final String ID_ATTR_NAME = "id";
    private static final String INDEX_ID_ATTR_NAME = "indexId";
    private CreateTableRequest request;

    @BeforeEach
    void beforeEach() {
        request = new CreateTableRequest()
                .withTableName("testTable")
                .withKeySchema(new KeySchemaElement(ID_ATTR_NAME, HASH))
                .withAttributeDefinitions(
                        new AttributeDefinition(ID_ATTR_NAME, S),
                        new AttributeDefinition(INDEX_ID_ATTR_NAME, S));
    }

    private static Stream<Arguments> argumentsBillingModeSetOnRequest() {
        return Stream.of(

            // Test when a request already has something set
            Arguments.of(BillingMode.PAY_PER_REQUEST, null, BillingMode.PAY_PER_REQUEST),
            Arguments.of(BillingMode.PAY_PER_REQUEST, null, BillingMode.PROVISIONED),
            Arguments.of(BillingMode.PROVISIONED, null, BillingMode.PAY_PER_REQUEST),
            Arguments.of(BillingMode.PROVISIONED, null, BillingMode.PROVISIONED),
            Arguments.of(null, null, BillingMode.PAY_PER_REQUEST),
            Arguments.of(null, null, BillingMode.PROVISIONED)
        );
    }

    private static Stream<Arguments> argumentsCapacitySetOnRequest() {
        return Stream.of(

            // Test when a request already has provisioned throughput set that provisioned throughput remains set
            Arguments.of(BillingMode.PAY_PER_REQUEST, new ProvisionedThroughput(5L, 5L)),
            Arguments.of(BillingMode.PAY_PER_REQUEST, new ProvisionedThroughput(1L, 1L)),
            Arguments.of(BillingMode.PAY_PER_REQUEST, new ProvisionedThroughput(0L, 0L)),

            Arguments.of(BillingMode.PROVISIONED, new ProvisionedThroughput(5L, 5L)),
            Arguments.of(BillingMode.PROVISIONED, new ProvisionedThroughput(1L, 1L)),
            Arguments.of(BillingMode.PROVISIONED, new ProvisionedThroughput(0L, 0L)),

            Arguments.of(null, new ProvisionedThroughput(5L, 5L)),
            Arguments.of(null, new ProvisionedThroughput(1L, 1L)),
            Arguments.of(null, new ProvisionedThroughput(0L, 0L))
        );
    }

    private static Stream<Arguments> argumentsNothingSetOnRequest() {
        return Stream.of(

            // Test when a request already has provisioned throughput set that provisioned throughput remains set

            Arguments.of(BillingMode.PAY_PER_REQUEST, null, BillingMode.PAY_PER_REQUEST),
            Arguments.of(BillingMode.PROVISIONED, null, BillingMode.PROVISIONED),
            Arguments.of(null, null, BillingMode.PROVISIONED)
        );
    }

    @ParameterizedTest(name = "{index} => billingModeInput={0}, requestProvisionedThroughput={1}, "
        + "requestBillingMode={2}")
    @MethodSource("argumentsBillingModeSetOnRequest")
    void testBillingModeWhenCreateTableRequestBillingModeIsSet(BillingMode billingModeInput,
                                                               ProvisionedThroughput inputProvisionedThroughput,
                                                               BillingMode expectedBillingMode) {

        assertNull(inputProvisionedThroughput);

        // Set Billing Mode for the create table request
        if (expectedBillingMode != null) {
            request.withBillingMode(expectedBillingMode);
        }

        // BillingMode should not be impacted by billingModeInput value since it's already set on the request
        DynamoDbCapacity.setBillingMode(request, billingModeInput);
        DynamoDbTestUtils.assertExpectedBillingModeIsSet(request, expectedBillingMode, 1L);
    }

    @ParameterizedTest(name = "{index} => billingModeInput={0}, requestProvisionedThroughput={1}")
    @MethodSource("argumentsCapacitySetOnRequest")
    void testBillingModeWhenCreateTableRequestProvisionedThroughputIsSet(BillingMode billingModeInput,
                                                                         ProvisionedThroughput
                                                                             inputProvisionedThroughput) {

        assertNotNull(inputProvisionedThroughput);
        String startingBillingMode = request.getBillingMode();
        assertNull(startingBillingMode);

        Long expectedCapacityUnits;
        expectedCapacityUnits = inputProvisionedThroughput.getReadCapacityUnits();
        request.withProvisionedThroughput(inputProvisionedThroughput);

        // ProvisionedThroughput should not be impacted by billingModeInput value since it's already set on the request
        DynamoDbCapacity.setBillingMode(request, billingModeInput);
        DynamoDbTestUtils.assertExpectedBillingModeIsSet(request, null, expectedCapacityUnits);
        assertNull(request.getBillingMode());
    }

    @ParameterizedTest(name = "{index} => billingModeInput={0}, requestProvisionedThroughput={1}, "
        + "requestBillingMode={2}")
    @MethodSource("argumentsNothingSetOnRequest")
    // No BillingMode/ProvisionedThroughput is set on the create table request
    void testBillingModeWhenCreateTableRequestIsNew(BillingMode billingModeInput,
                                                    ProvisionedThroughput inputProvisionedThroughput,
                                                    BillingMode expectedBillingMode) {
        assertNull(inputProvisionedThroughput);

        DynamoDbCapacity.setBillingMode(request, billingModeInput);
        DynamoDbTestUtils.assertExpectedBillingModeIsSet(request, expectedBillingMode, 1L);
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
}

