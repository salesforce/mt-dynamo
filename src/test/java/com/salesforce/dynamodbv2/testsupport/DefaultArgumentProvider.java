package com.salesforce.dynamodbv2.testsupport;

import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

/**
 * Default implementation of the ArgumentProvider interface.  It is meant to be referenced in
 * a @ParameterizedTest's @ArgumentProvider annotation.
 *
 * <p>It delegates to the ArgumentBuilder to get a list of TestArguments.  Each TestArgument will be used as an input
 * to a test invocation.  Before returning the list of TestArgument's, it calls setTestSetup on its DefaultTestSetup.
 * The DefaultTestSetup implementation creates tables for each TestArgument's org/AmazonDynamoDB/hashKeyAttrType
 * combination.
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
        List<Arguments> arguments = argumentBuilder.get();
        arguments.forEach(arguments1 -> testSetup.setupTest((TestArgument) arguments1.get()[0]));
        return arguments.stream();
    }

}