package com.salesforce.dynamodbv2.testsupport;

import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

/**
 * Default implementation of the ArgumentProvider interface.  It is meant to be referenced in
 * a @ParameterizedTest's @ArgumentProvider annotation.
 *
 * <p>It delegates to the ArgumentBuilder to get a list of TestArguments.  Each TestArgument will be used as an input
 * to a test invocation.  Before returning the list of TestArgument's, it calls accept on its DefaultTestSetup.
 * The DefaultTestSetup implementation creates tables for each TestArgument's org/AmazonDynamoDB/hashKeyAttrType
 * combination.
 *
 * @author msgroi
 */
public class DefaultArgumentProvider implements ArgumentsProvider {

    private ArgumentBuilder argumentBuilder = new ArgumentBuilder();
    private TestSetup testSetup = new DefaultTestSetup();
    private List<TestArgument> testArguments = new ArrayList<>();

    void setArgumentBuilder(ArgumentBuilder argumentBuilder) {
        this.argumentBuilder = argumentBuilder;
    }

    protected void setTestSetup(TestSetup testSetup) {
        this.testSetup = testSetup;
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
        List<Arguments> arguments = argumentBuilder.get();
        testArguments.addAll(arguments.stream().map(arguments1 ->
            (TestArgument) arguments1.get()[0]).collect(Collectors.toList()));
        testArguments.forEach(testArgument -> testSetup.accept(testArgument));
        return arguments.stream();
    }

}