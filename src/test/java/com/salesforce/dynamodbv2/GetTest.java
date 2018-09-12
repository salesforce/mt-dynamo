package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItemDefaultHkRk;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests getItem().
 *
 * @author msgroi
 */
class GetTest {

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void get(TestArgument testArgument) {
        testArgument.forEachOrgContext(
            org -> assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE1),
                is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                    SOME_FIELD_VALUE + TABLE1 + org))));
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    void getHkRkTable(TestArgument testArgument) {
        testArgument.forEachOrgContext(
            org -> assertThat(getItemDefaultHkRk(testArgument.getHashKeyAttrType(),
                    testArgument.getAmazonDynamoDb(),
                    TABLE3),
                is(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                    SOME_FIELD_VALUE + TABLE3 + org))));
    }

}