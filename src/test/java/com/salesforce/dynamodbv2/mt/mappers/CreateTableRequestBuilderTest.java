package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;
import static java.util.Optional.empty;

import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.util.DynamoDbTestUtils;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CreateTableRequestBuilderTest {

    private static final String HASH_KEY_FIELD = "hk";
    private static final String RANGE_KEY_FIELD = "rk";
    private PrimaryKey primaryKey;
    private String indexName;
    private DynamoSecondaryIndex.DynamoSecondaryIndexType indexType;

    private CreateTableRequestBuilder testBuilder;

    private static Stream<Arguments> arguments() {
        return Stream.of(
            Arguments.of(new ProvisionedThroughput(5L, 5L), BillingMode.PAY_PER_REQUEST),
            Arguments.of(new ProvisionedThroughput(5L, 5L), BillingMode.PROVISIONED),
            Arguments.of(new ProvisionedThroughput(5L, 5L), null),

            Arguments.of(new ProvisionedThroughput(1L, 1L), BillingMode.PAY_PER_REQUEST),
            Arguments.of(new ProvisionedThroughput(1L, 1L), BillingMode.PROVISIONED),
            Arguments.of(new ProvisionedThroughput(1L, 1L), null),

            Arguments.of(new ProvisionedThroughput(0L, 0L), BillingMode.PAY_PER_REQUEST),
            Arguments.of(new ProvisionedThroughput(0L, 0L), BillingMode.PROVISIONED),
            Arguments.of(new ProvisionedThroughput(0L, 0L), null),

            Arguments.of(null, BillingMode.PAY_PER_REQUEST),
            Arguments.of(null, BillingMode.PROVISIONED),
            Arguments.of(null, null)
        );
    }

    @BeforeEach
    void beforeEach() {

        testBuilder = CreateTableRequestBuilder.builder()
            .withTableName("testBuilder")
            .withTableKeySchema(HASH_KEY_FIELD, S, RANGE_KEY_FIELD, S);

        indexType = GSI;
        ScalarAttributeType hashKeyType = S;
        Optional<ScalarAttributeType> rangeKeyType = empty();

        indexName = indexType.name().toLowerCase() + "_"
            + hashKeyType.name().toLowerCase()
            + rangeKeyType.map(type -> "_" + type.name().toLowerCase()).orElse("").toLowerCase();
        primaryKey = rangeKeyType.map(
            scalarAttributeType -> new PrimaryKey(indexType == LSI ? "hk" : indexName + "_hk",
                hashKeyType,
                indexName + "_rk",
                scalarAttributeType))
            .orElseGet(() -> new PrimaryKey(indexName + "_hk",
                hashKeyType));
    }

    @ParameterizedTest(name = "{index} => capacityUnits={0}, expectedBillingMode={1}")
    @MethodSource("arguments")
    void testExpectedBillingMode(ProvisionedThroughput inputProvisionedThroughput, BillingMode expectedBillingMode) {

        Long expectedCapacityUnits = null;
        if (inputProvisionedThroughput != null && (expectedBillingMode == null
            || expectedBillingMode.equals(BillingMode.PROVISIONED))) {

            expectedCapacityUnits = inputProvisionedThroughput.getReadCapacityUnits();
            testBuilder.withProvisionedThroughput(expectedCapacityUnits, expectedCapacityUnits);
        }

        if (inputProvisionedThroughput == null && (expectedBillingMode == null
                || expectedBillingMode.equals(BillingMode.PROVISIONED))) {
            expectedCapacityUnits = 1L;
        }

        testBuilder.withBillingMode(expectedBillingMode);
        testBuilder.addSi(indexName, indexType, primaryKey, expectedCapacityUnits);
        testBuilder.build();

        DynamoDbTestUtils.assertExpectedBillingModeIsSet(
            testBuilder.getCreateTableRequest(), expectedBillingMode, expectedCapacityUnits);
    }
}