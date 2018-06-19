/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.dynamodb;

import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/*
 * @author msgroi
 */
class DynamoDBRetryTest {

    @Test
    void test() {
        @SuppressWarnings("unchecked")
        Function<Object, Object> myfunction = mock(Function.class);
        new DynamoDBRetry(() -> myfunction.apply(null)).execute();
        verify(myfunction, times(1)).apply(null);
    }

    @Test
    void testLimitedExceededException() {
        @SuppressWarnings("unchecked")
        Function<Object, Object> myfunction = mock(Function.class);
        new DynamoDBRetry(() -> {
            myfunction.apply(null);
            throw new LimitExceededException("intentional exception");
        }, 5, 1, 1).execute();
        verify(myfunction, times(5)).apply(null);
    }

    @Test
    void testOtherException() {
        try {
            new DynamoDBRetry(() -> {
                throw new RuntimeException("intentional exception");
            }).execute();
            fail("expected exception not encountered");
        } catch (RuntimeException e) {
            assertEquals("java.lang.RuntimeException: intentional exception", e.getMessage());
        }
    }
}