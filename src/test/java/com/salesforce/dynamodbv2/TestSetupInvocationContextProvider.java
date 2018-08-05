package com.salesforce.dynamodbv2;

import com.salesforce.dynamodbv2.TestArgumentSupplier.TestArgument;

/**
 * Requirements ...
 * - Each test method should be run once per MTAmazonDynamoDB configuration variant being tested.
 * - Each test method should be run with set of orgs that are designated for use by that test.
 * - Before each test method is called, a default set of tables created and populated with default set of data.
 * - A test method should be able to a override or extend the table and/or data set up scripts.
 * - Use JUnit 5 since mt-dynamo has been using it since its inception.
 *
 * Solution
 *
 * Use JUnit 5 Test Templates(https://junit.org/junit5/docs/current/user-guide/#writing-tests-test-templates).
 * Test templates allow you to define a test that can be invoked multiple times.  To use test templates, you provide
 * an implementation of the TestTemplateInvocationContextProvider interface.  That implementation returns a list
 * of invocation contexts.
 *
 * To use ...
 *
 * Annotate your test class or test method with @ExtendWith(TestSetupInvocationContextProvider.class).  Each test
 * method be annotated with @TestTemplate and must take a single argument of type TestArgument.
 *
 * FAQ
 *
 * - Q. Why not use ParameterizedTest's and JUnit 5 lifecycle callback methods at the class level to create and
 * populate tables in the beforeEach method callback?
 * - A. This works if every test method in a class can use the same script to populate data.  This is contrary to our
 * requirement.
 *
 * - Q. Why not use ParameterizedTest's and per method lifecycle methods(@RegisterExtension)?
 * - A. JUnit 5 lifecycle callback methods(@ExtendsWith) do not have access to parameterized test arguments.  Therefore,
 * there is no hook where pre-test data population can occur.  Note that this is a known deficiency that's currently slated
 * to be addressed in an upcoming version of JUnit 5(https://github.com/junit-team/junit5/issues/1139).
 *
 * - Q. Why not use ParameterizedTest's but just create the tables and data at the beginning of each test?
 * - A. This is a viable solution, but it would mean adding a couple of lines of code to every test method in every class.
 *
 * @author msgroi
 */
class TestSetupInvocationContextProvider extends InvocationContextProviderWithSetupSupport<TestArgument> {

    private static TestSetup testSetup = new TestSetup();

    TestSetupInvocationContextProvider() {
        super(
            TestArgument.class, // the model class that encapsulates a single invocation of your test
            new TestArgumentSupplier().get(), // supplier that returns a list of TestArgument instances each or which will result in a test invocation
            testSetup.getSetup(), // setups up tables and populates with data
            testSetup.getTeardown() // tears down tables
        );
    }

}