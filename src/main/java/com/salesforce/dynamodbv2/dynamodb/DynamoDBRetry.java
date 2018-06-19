/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.dynamodb;

import com.amazonaws.services.dynamodbv2.model.LimitExceededException;

import static java.lang.Math.pow;

/*
 * @author msgroi
 */
public class DynamoDBRetry {

    private final int maxRetries;
    private final int initialBackoffMS;
    private final int backoffMultiplier;
    private final DynamoDBRetriable retriable;

    public DynamoDBRetry(DynamoDBRetriable retriable) {
        this.retriable = retriable;
        this.maxRetries = 6;
        this.initialBackoffMS = 1000;
        this.backoffMultiplier = 2;
    }

    DynamoDBRetry(DynamoDBRetriable retriable,
                  int maxRetries,
                  int initialBackoffMS,
                  int backoffMultiplier) {
        this.retriable = retriable;
        this.maxRetries = maxRetries;
        this.initialBackoffMS = initialBackoffMS;
        this.backoffMultiplier = backoffMultiplier;
    }

    public void execute() {
        int tries = 0;
        while (tries < maxRetries) {
            try {
                tries++;
                retriable.run();
                return;
            } catch (LimitExceededException e) {
                sleep((int) (initialBackoffMS + (pow(backoffMultiplier, tries + 1))));
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }
    }

    @FunctionalInterface
    public interface DynamoDBRetriable {
        void run();
    }

    private void sleep(int ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignore) {}
    }

}