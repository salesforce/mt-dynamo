package com.salesforce.dynamodbv2.mt.context.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for context behavior.
 */
class MtAmazonDynamoDbContextProviderThreadLocalImplTest {

    MtAmazonDynamoDbContextProviderThreadLocalImpl sut;

    @BeforeEach
    void before() {
        sut = new MtAmazonDynamoDbContextProviderThreadLocalImpl();
        sut.setContext(null);
    }

    @AfterEach
    void after() {
        sut.setContext(null);
    }

    @Test
    void testNullOpt() {
        assertTrue(sut.getContextOpt().isEmpty());
    }

    @Test
    void testNull() {
        assertThrows(IllegalStateException.class, () -> sut.getContext());
    }

    @Test
    void testValidContext() {
        final String ctx = "abc";
        sut.setContext(ctx);
        assertEquals(ctx, sut.getContext());
        assertEquals(Optional.of(ctx), sut.getContextOpt());
    }

    @Test
    void testInvalidContext() {
        assertThrows(IllegalArgumentException.class, () -> sut.setContext("a/b"));
    }

    @Test
    void testWith() {
        final String ctx = "1";
        sut.withContext(ctx, () -> assertEquals(ctx, sut.getContext()));
    }

    @Test
    void testWithF() {
        final String ctx = "1";
        final String input = "a";
        final String output = "b";
        assertEquals(output, sut.withContext(ctx, p -> {
            assertEquals(input, p);
            assertEquals(Optional.of(ctx), sut.getContextOpt());
            return output;
        }, input));
    }
}
