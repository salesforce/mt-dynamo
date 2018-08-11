package com.salesforce.dynamodbv2.testsupport;

import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

/**
 * Default implementation of the ArgumentProvider interface.  It is meant to be referenced in a @ParameterizedTest's
 * &#064;ArgumentProvider annotation.
 *
 * <p>It delegates to the ArgumentBuilder to get a list of TestArguments.  Each TestArgument will be used as an input
 * to a test invocation.  Before returning the list of TestArgument's, it calls setTestSetup on its DefaultTestSetup.
 * The DefaultTestSetup implementation creates tables for each TestArgument's org/AmazonDynamoDB/hashKeyAttrType
 * combination.
 *
 * <p>To use with the DefaultArgumentBuilder and DefaultTestSetup add the following annotation on your test ...
 *
 *     <p>&#064;ParameterizedTest
 *     &#064;ArgumentsSource(DefaultArgumentProvider.class)
 *
 * <p>To use with a custom ArgumentBuilder and custom TestSetup, declare a static inner class ...
 *
 *     <p>static class MyArgumentProvider extends DefaultArgumentProvider {
 *         public MyArgumentProvider() {
 *             super(new MyArgumentBuilder(), new MyTestSetup());
 *         }
 *     }
 *
 *     <p>... then reference that class in your test method annotation ...
 *
 *     <p>&#064;ParameterizedTest
 *     &#064;ArgumentsSource(MyArgumentProvider.class)
 *
 * <p>To use with a custom ArgumentBuilder and default TestSetup, declare a static inner class ...
 *
 *     <p>static class MyArgumentProvider extends DefaultArgumentProvider {
 *         public MyArgumentProvider() {
 *             super(new MyArgumentBuilder());
 *         }
 *     }
 *
 * <p>To use with the default ArgumentBuilder and a custom TestSetup, declare a static inner class ...
 *
 *     <p>static class MyArgumentProvider extends DefaultArgumentProvider {
 *         public MyArgumentProvider() {
 *             super(new MyTestSetup());
 *         }
 *     }
 *
 * <p>Of course, if you want the default test data plus the ability to add your own, in the above example, MyTestSetup
 * could extend DefaultTestSetup and the setupTest() implementation could call super.setupTest() before adding its own
 * data.
 *
 * @author msgroi
 */
public class DefaultArgumentProvider implements ArgumentsProvider {

    private final ArgumentBuilder argumentBuilder;
    private final TestSetup testSetup;

    /**
     * Used when used with an arguments source annotation directly, e.g. @ArgumentsSource(DefaultArgumentProvider.class)
     */
    public DefaultArgumentProvider() {
        this(new ArgumentBuilder(), new DefaultTestSetup());
    }

    public DefaultArgumentProvider(TestSetup testSetup) {
        this.argumentBuilder = new ArgumentBuilder();
        this.testSetup = testSetup;
    }

    public DefaultArgumentProvider(ArgumentBuilder argumentBuilder, TestSetup testSetup) {
        this.argumentBuilder = argumentBuilder;
        this.testSetup = testSetup;
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
        List<TestArgument> arguments = argumentBuilder.get();
        /*
         * Before returning the list of Arguments back to JUnit so it can pass each in as a test invocation, we
         * are calling TestSetup.setupTest().  This provides a hook for doing additional data setup for each
         * parameterized test argument.  The default TestSetup implementation, DefaultTestSetup, creates a default
         * set of tables and populates them with a default set of data, which leads to the question, why not declare a
         * standard JUnit BeforeEachCallback handler and perform the data set up there.  The answer is that the
         * ExtensionContext provided in JUnit callback methods do not have access to @ParameterizedTest arguments.
         * Note that this is a known deficiency which is currently slated to be addressed in an upcoming version of
         * JUnit 5(https://github.com/junit-team/junit5/issues/1139).
         */
        arguments.forEach(testSetup::setupTest);
        return arguments.stream().map(Arguments::of);
    }

}