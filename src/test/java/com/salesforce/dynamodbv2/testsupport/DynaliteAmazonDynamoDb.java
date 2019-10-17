/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.testsupport;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.base.Suppliers;
import java.util.function.Supplier;
import org.testcontainers.dynamodb.DynaliteContainer;

/**
 * Provides an alternative local Dynamo DB implementation that we can use in cases where the official DynamoDB Local
 * has bugs and deviates from remote DynamoDB behavior. This involves starting a Dynalite docker image, which gets
 * shut down automatically when the JVM stops. Tests using this take significantly longer to run, so we should use this
 * sparingly.
 *
 * <p>See: https://www.testcontainers.org/modules/databases/dynalite/ and https://github.com/mhart/dynalite
 */
class DynaliteAmazonDynamoDb {

    private static final Supplier<DynaliteContainer> dynaliteContainer =
        Suppliers.memoize(DynaliteAmazonDynamoDb::initialize);

    private static DynaliteContainer initialize() {
        DynaliteContainer container = new DynaliteContainer();
        container.start();
        return container;
    }

    static AmazonDynamoDB getAmazonDynamoDB() {
        return dynaliteContainer.get().getClient();
    }
}
