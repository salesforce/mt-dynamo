package com.salesforce.dynamodbv2.testsupport;

import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

/**
 * Default implementation of the {@code ArgumentProvider} interface.  It is meant to be referenced in a
 * {@code @ParameterizedTest}'s {@code @ArgumentProvider} annotation.
 *
 * <p>It delegates to the {@code ArgumentBuilder} to get a list of {@code TestArgument}s.  Each {@code TestArgument}
 * will be used as an input to a test invocation.  Before returning the list of {@code TestArgument}s, it calls
 * {@code setTestSetup} on its {@code DefaultTestSetup}.  The {@code DefaultTestSetup} implementation creates tables for
 * each {@code TestArgument}'s {@code org}/{@code AmazonDynamoDB}/{@code hashKeyAttrType} combination.
 *
 * <p>To use with the {@code DefaultArgumentBuilder} and {@code DefaultTestSetup} add the following annotation on your
 * test ...
 *
 *     <p>{@code @ParameterizedTest
 *     @ArgumentsSource(DefaultArgumentProvider.class)
 *     }
 *
 * <p>To use with a custom {@code ArgumentBuilder} and custom {@code TestSetup}, declare a static inner class ...
 *
 *     <p>{@code static class MyArgumentProvider extends DefaultArgumentProvider {
 *         public MyArgumentProvider() {
 *             super(new MyArgumentBuilder(), new MyTestSetup());
 *         }
 *     }
 *     }
 *
 *     <p>... then reference that class in your test method annotation ...
 *
 *     <p>{@code ParameterizedTest
 *     @ArgumentsSource(MyArgumentProvider.class)
 *     }
 *
 * <p>To use with a custom {@code ArgumentBuilder} and default {@code TestSetup}, declare a static inner class ...
 *
 *     <p>{@code static class MyArgumentProvider extends DefaultArgumentProvider {
 *         public MyArgumentProvider() {
 *             super(new MyArgumentBuilder());
 *         }
 *     }
 *     }
 *
 * <p>To use with the default {@code ArgumentBuilder} and a custom {@code TestSetup}, declare a static inner class ...
 *
 *     <p>{@code static class MyArgumentProvider extends DefaultArgumentProvider {
 *         public MyArgumentProvider() {
 *             super(new MyTestSetup());
 *         }
 *     }
 *     }
 *
 * <p>Of course, if you want the default test data plus the ability to add your own, in the above example,
 * {@code MyTestSetup} could extend {@code DefaultTestSetup} and the {@code setupTest()} implementation could call
 * {@code super.setupTest()} before adding its own data.
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