package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_OTHER_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_OTHER_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithValues;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableSet;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.TestSupport;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests batchGetItem().
 */
class BatchGetTest {

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void batchGet(TestArgument testArgument) {
        testArgument.forEachOrgContext(
            org -> {
                final List<String> hashKeyValues = Arrays.asList(HASH_KEY_VALUE, HASH_KEY_OTHER_VALUE);
                final Set<Map<String, AttributeValue>> gottenItems = TestSupport.batchGetItem(
                        testArgument.getHashKeyAttrType(),
                        testArgument.getAmazonDynamoDb(),
                        TABLE1,
                        hashKeyValues,
                        Optional.empty());
                final Map<String, AttributeValue> expectedItem0 = buildItemWithValues(
                        testArgument.getHashKeyAttrType(),
                        hashKeyValues.get(0),
                        Optional.empty(),
                        SOME_FIELD_VALUE + TABLE1 + org);
                final Map<String, AttributeValue> expectedItem1 = buildItemWithValues(
                        testArgument.getHashKeyAttrType(),
                        hashKeyValues.get(1),
                        Optional.empty(),
                        SOME_OTHER_FIELD_VALUE + TABLE1 + org);
                assertEquals(ImmutableSet.of(expectedItem0, expectedItem1), gottenItems);
            });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void batchGetHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(
            org -> {
                final List<String> hashKeyValues = Arrays.asList(HASH_KEY_VALUE, HASH_KEY_VALUE);
                final Optional<List<String>> rangeKeyValues = Optional
                        .of(Arrays.asList(RANGE_KEY_VALUE, RANGE_KEY_VALUE + "2"));
                final Set<Map<String, AttributeValue>> gottenItems = TestSupport.batchGetItem(
                        testArgument.getHashKeyAttrType(),
                        testArgument.getAmazonDynamoDb(),
                        TABLE3,
                        hashKeyValues,
                        rangeKeyValues);
                final Map<String, AttributeValue> expectedItem0 = buildItemWithValues(
                        testArgument.getHashKeyAttrType(),
                        hashKeyValues.get(0),
                        rangeKeyValues.map(rkv -> rkv.get(0)),
                        SOME_FIELD_VALUE + TABLE3 + org);
                final Map<String, AttributeValue> expectedItem1 = buildItemWithValues(
                        testArgument.getHashKeyAttrType(),
                        hashKeyValues.get(1),
                        rangeKeyValues.map(rkv -> rkv.get(1)),
                        SOME_FIELD_VALUE + TABLE3 + org + "2",
                        Optional.of(INDEX_FIELD_VALUE));
                assertEquals(ImmutableSet.of(expectedItem0, expectedItem1), gottenItems);
            });
    }

}