/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.context;

import java.util.function.Function;

/**
 * Interface that holds the tenant context.
 *
 * @author msgroi
 */
@FunctionalInterface
public interface MtAmazonDynamoDbContextProvider {

    String getContext();

    /**
     * Sets the tenant context.
     */
    default void setContext(String tenantId) {
        // defaults to no-op
    }

    /**
     * Sets the context to the specific tenantId, executes the runnable, resets back to original tenantId.
     */
    default void withContext(String tenantId, Runnable runnable) {
        String origContext = getContext();
        try {
            setContext(tenantId);
            runnable.run();
        } finally {
            setContext(origContext);
        }
    }

    /**
     * Sets the context to the specified tenantId, executes the function with the given argument, resets back to the
     * original tenantId, and returns the result of calling the function.
     *
     * @param tenantId Context tenantId to use when calling the function.
     * @param function Function to call within tenant context.
     * @param t        Parameter to function.
     * @param <T>      Input type of function.
     * @param <R>      Output type of function.
     * @return
     */
    default <T, R> R withContext(String tenantId, Function<T, R> function, T t) {
        String origContext = getContext();
        setContext(tenantId);
        try {
            return function.apply(t);
        } finally {
            setContext(origContext);
        }
    }

}
