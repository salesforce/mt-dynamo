package com.salesforce.dynamodbv2.testsupport;

import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;

/**
 * Implement this interface to provide your own test set up implementation.
 *
 * @author msgroi
 */
interface TestSetup {

    void setupTest(TestArgument testArgument);

}