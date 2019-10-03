package com.salesforce.dynamodbv2;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE5;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.HASH_KEY_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.RANGE_KEY_S_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider.DefaultArgumentProviderForTable1;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider.DefaultArgumentProviderForTable3;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider.DefaultArgumentProviderForTable5;
import com.salesforce.dynamodbv2.testsupport.ItemBuilder;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests getItem().
 *
 * @author msgroi
 */
class GetTest {

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProviderForTable1.class)
    void get(TestArgument testArgument) {
        testArgument.forEachOrgContext(
            org -> assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + TABLE1 + org)
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE1,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.empty())));
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProviderForTable3.class)
    void getHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(
            org -> assertEquals(ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                .someField(S, SOME_FIELD_VALUE + TABLE3 + org)
                .rangeKey(S, RANGE_KEY_S_VALUE)
                .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    TABLE3,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE))));
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProviderForTable5.class)
    void getHkRk_TableWithGsiHkSameAsTableRk(TestArgument testArgument) {
        String table = TABLE5;
        testArgument.forEachOrgContext(
            org -> assertEquals(
                ItemBuilder.builder(testArgument.getHashKeyAttrType(), HASH_KEY_VALUE)
                    .someField(S, SOME_FIELD_VALUE + table + org)
                    .rangeKey(S, RANGE_KEY_S_VALUE)
                    .build(),
                getItem(testArgument.getAmazonDynamoDb(),
                    table,
                    HASH_KEY_VALUE,
                    testArgument.getHashKeyAttrType(),
                    Optional.of(RANGE_KEY_S_VALUE))));
    }

}