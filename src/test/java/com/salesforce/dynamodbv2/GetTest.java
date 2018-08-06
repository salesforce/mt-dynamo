package com.salesforce.dynamodbv2;

import static com.salesforce.dynamodbv2.TestSetup.TABLE1;
import static com.salesforce.dynamodbv2.TestSetup.TABLE3;
import static com.salesforce.dynamodbv2.TestSupport.SOME_FIELD_VALUE;
import static com.salesforce.dynamodbv2.TestSupport.buildHkRkItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.TestSupport.buildItemWithSomeFieldValue;
import static com.salesforce.dynamodbv2.TestSupport.getHkRkItem;
import static com.salesforce.dynamodbv2.TestSupport.getItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.salesforce.dynamodbv2.TestArgumentSupplier.TestArgument;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * @author msgroi
 */
@ExtendWith(TestTemplateWithDataSetup.class)
class GetTest {

    private static final MtAmazonDynamoDbContextProvider MT_CONTEXT = TestArgumentSupplier.MT_CONTEXT;

    @TestTemplate
    void get(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            assertThat(buildItemWithSomeFieldValue(SOME_FIELD_VALUE + TABLE1 + org),
                       is(getItem(testArgument.getAmazonDynamoDB(), TABLE1)));
        });
    }

    @TestTemplate
    void getHkRkTable(TestArgument testArgument) {
        testArgument.getOrgs().forEach(org -> {
            MT_CONTEXT.setContext(org);
            assertThat(buildHkRkItemWithSomeFieldValue(SOME_FIELD_VALUE + TABLE3 + org),
                       is(getHkRkItem(testArgument.getAmazonDynamoDB(), TABLE3)));
        });
    }

}