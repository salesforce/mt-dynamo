package com.salesforce.dynamodbv2.testsupport;

import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier.TestArgument;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

/**
 * Implements the ArgumentProvider interface so it can be referenced in an @ArgumentProvider annotation.
 *
 * @author msgroi
 */
public class TestArgumentProvider extends TestSetup implements ArgumentsProvider {

    private TestArgumentSupplier testArgumentSupplier = new TestArgumentSupplier();
    private TestSetup testSetup = new TestSetup();
    private List<TestArgument> testArguments = new ArrayList<>();

    void setTestArgumentSupplier(TestArgumentSupplier testArgumentSupplier) {
        this.testArgumentSupplier = testArgumentSupplier;
    }

    protected void setTestSetup(TestSetup testSetup) {
        this.testSetup = testSetup;
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
        List<Arguments> arguments = testArgumentSupplier.get();
        testArguments.addAll(arguments.stream().map(arguments1 ->
            (TestArgument) arguments1.get()[0]).collect(Collectors.toList()));
        testArguments.forEach(testArgument -> testSetup.getSetup().accept(testArgument));
        return arguments.stream();
    }

    /**
     * Tears down all tables created.
     */
    public void teardown() {
        testArguments.forEach(testArgument -> testSetup.getTeardown().accept(testArgument));
    }

}