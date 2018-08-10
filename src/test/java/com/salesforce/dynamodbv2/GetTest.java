package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.testsupport.TestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.TestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getHkRkItem;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.getItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.testsupport.TestArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier;
import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier.TestArgument;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests getItem().
 *
 * @author msgroi
 */
class GetTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;

    @ParameterizedTest
    @ArgumentsSource(TestArgumentProvider.class)
    void get(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE1),
                       is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                           SOME_FIELD_VALUE + TABLE1 + org)));
        });
    }

    @ParameterizedTest
    @ArgumentsSource(TestArgumentProvider.class)
    void getHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3),
                       is(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                           SOME_FIELD_VALUE + TABLE3 + org)));
        });
    }

}