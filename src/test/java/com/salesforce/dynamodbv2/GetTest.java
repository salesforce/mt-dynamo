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

import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier;
import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier.TestArgument;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests getItem().
 *
 * @author msgroi
 */
@ExtendWith(TestTemplateWithDataSetup.class)
class GetTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;

    @TestTemplate
    void get(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            assertThat(getItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE1),
                       is(buildItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                           SOME_FIELD_VALUE + TABLE1 + org)));
        });
    }

    @TestTemplate
    void getHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            assertThat(getHkRkItem(testArgument.getHashKeyAttrType(), testArgument.getAmazonDynamoDb(), TABLE3),
                       is(buildHkRkItemWithSomeFieldValue(testArgument.getHashKeyAttrType(),
                           SOME_FIELD_VALUE + TABLE3 + org)));
        });
    }

}