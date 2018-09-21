package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_OTHER_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.INDEX_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_OTHER_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_OTHER_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_OTHER_OTHER_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.batchGetItem;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableSet;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;

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
                final List<Optional<String>> rangeKeyValueOpts = Arrays.asList(Optional.empty(), Optional.empty());
                final Set<Map<String, AttributeValue>> gottenItems = batchGetItem(testArgument.getHashKeyAttrType(),
                        testArgument.getAmazonDynamoDb(),
                        TABLE1,
                        hashKeyValues,
                        rangeKeyValueOpts);
                final Map<String, AttributeValue> expectedItem0 = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                            hashKeyValues.get(0))
                        .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                        .build();
                final Map<String, AttributeValue> expectedItem1 = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                            hashKeyValues.get(1))
                        .someField(S, SOME_OTHER_OTHER_FIELD_VALUE + TABLE1 + org)
                        .build();
                assertEquals(ImmutableSet.of(expectedItem0, expectedItem1), gottenItems);
            });
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void batchGetHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(
            org -> {
                final List<String> hashKeyValues = Arrays.asList(HASH_KEY_VALUE, HASH_KEY_VALUE);
                final List<Optional<String>> rangeKeyValueOpts = Arrays.asList(Optional.of(RANGE_KEY_S_VALUE),
                        Optional.of(RANGE_KEY_OTHER_S_VALUE));
                final Set<Map<String, AttributeValue>> gottenItems = batchGetItem(testArgument.getHashKeyAttrType(),
                        testArgument.getAmazonDynamoDb(),
                        TABLE3,
                        hashKeyValues,
                        rangeKeyValueOpts);
                final Map<String, AttributeValue> expectedItem0 = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                            hashKeyValues.get(0))
                        .someField(S, SOME_FIELD_VALUE + TABLE3 + org)
                        .rangeKey(S, RANGE_KEY_S_VALUE)
                        .build();
                final Map<String, AttributeValue> expectedItem1 = ItemBuilder.builder(testArgument.getHashKeyAttrType(),
                            hashKeyValues.get(1))
                        .someField(S, SOME_OTHER_FIELD_VALUE + TABLE3 + org)
                        .rangeKey(S, RANGE_KEY_OTHER_S_VALUE)
                        .indexField(S, INDEX_FIELD_VALUE)
                        .build();
                assertEquals(ImmutableSet.of(expectedItem0, expectedItem1), gottenItems);
            });
    }
}
