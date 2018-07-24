/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.context;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
@FunctionalInterface
public interface MtAmazonDynamoDbContextProvider {

    String getContext();

    default void setContext(String tenantId) {
        // defaults to no-op
    }
}
