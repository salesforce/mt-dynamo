package com.salesforce.dynamodbv2.mt.util;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Iterables;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit test for composite strings util.
 */
class CompositeStringsTest {

    private static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(singleton(""), ""),
                Arguments.of(singleton("abc"), "abc"),
                Arguments.of(singleton("a-bc"), "a\\-bc"),
                Arguments.of(singleton("a\\-bc"), "a\\\\\\-bc"),
                Arguments.of(singleton("abc\\"), "abc\\\\"),
                Arguments.of(asList("a", "b", "c"), "a-b-c"),
                Arguments.of(asList("a-", "-b", "c"), "a\\--\\-b-c"),
                Arguments.of(asList("a\\-", "-b", ""), "a\\\\\\--\\-b-"),
                Arguments.of(asList("\\", "\\", "\\"), "\\\\-\\\\-\\\\")
        );
    }

    private final CompositeStrings compositeStrings = new CompositeStrings();

    @ParameterizedTest
    @MethodSource("data")
    void test(Iterable<String> keys, String expected) {
        String join = compositeStrings.join(keys);
        assertEquals(expected, join);

        Iterable<String> split = () -> compositeStrings.split(join);
        assertTrue(Iterables.elementsEqual(keys, split));
    }

}
