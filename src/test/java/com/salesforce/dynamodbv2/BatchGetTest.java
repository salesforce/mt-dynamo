package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_OTHER_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_OTHER_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_OTHER_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_OTHER_OTHER_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.batchGetItem;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.google.common.collect.ImmutableSet;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

    /**
     * Tests that partial results returned by batch gets are handled properly.  When partial results are returned, the
     * {@code BatchGetItemOutcome} will contain a map of the UnprocessedKeys.  Partial results will be returned if
     * the response size limit is exceeded, the table's provisioned throughput is exceeded, or an internal processing
     * failure has occurred.  This test triggers partial results by exceeding the 16MB response size limit.  It does
     * this by POSTing 100 records that are ~300KB in size(~20MB in total).  It then attempts to retrieve them in a
     * single batch GET call.  Partial results are returned because retrieving all 100 records in a single batch get
     * exceeds the 16MB response size limit.
     *
     * <p>For more info, see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html.
     */
    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void batchGetWithUnprocessedKeys(TestArgument testArgument) {
        testArgument.forEachOrgContext(
            org -> {
                final int targetRecordLength = 300 * 1024; // 300KB record
                final int itemCount = 100;

                // write records
                IntStream.rangeClosed(1, itemCount).forEach(i -> {
                    testArgument.getAmazonDynamoDb().putItem(new PutItemRequest().withTableName(TABLE1).withItem(
                        ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_OTHER_VALUE + i)
                            .someField(S, "x".repeat(targetRecordLength)).build()));
                });

                // read records in batch
                batchGetItem(testArgument.getHashKeyAttrType(),
                    testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    IntStream.rangeClosed(1, itemCount).boxed()
                        .map(i -> HASH_KEY_OTHER_VALUE + i).collect(Collectors.toList()),
                    IntStream.rangeClosed(1, itemCount).boxed()
                        .map((Function<Integer, Optional<String>>) i -> Optional.empty()).collect(Collectors.toList()));
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
                        .withDefaults()
                        .build();
                assertEquals(ImmutableSet.of(expectedItem0, expectedItem1), gottenItems);
            });
    }
}
