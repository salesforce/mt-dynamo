package com.salesforce.dynamodbv2.testsupport;

import com.salesforce.dynamodbv2.testsupport.TestArgumentSupplier.TestArgument;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

/**
 * Implements the ArgumentProvider interface so it can be referenced in an @ArgumentProvider annotation.
 *
 * // TODO absord TestArgumentSupplier into this once we've cleared out the @TestTemplate tests
 *
 * @author msgroi
 */
public class ParameterizedTestArgumentProvider extends TestSetup implements ArgumentsProvider { // TODO msgroi rename taking away ParameterizedTest prefix

    private TestArgumentSupplier testArgumentSupplier = new TestArgumentSupplier();
    private Consumer<TestArgument> beforeCallback = testArgument -> getSetup().accept(testArgument);

    void setTestArgumentSupplier(TestArgumentSupplier testArgumentSupplier) {
        this.testArgumentSupplier = testArgumentSupplier;
    }

    protected void setBeforeCallback(
        Consumer<TestArgument> beforeCallback) {
        this.beforeCallback = beforeCallback;
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
        List<Arguments> arguments = testArgumentSupplier.get();
        arguments.forEach(arguments1 -> beforeCallback.accept((TestArgument) arguments1.get()[0]));
        return arguments.stream();
    }

}