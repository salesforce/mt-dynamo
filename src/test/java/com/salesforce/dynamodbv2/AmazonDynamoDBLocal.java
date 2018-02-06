/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;

/**
 * @author msgroi
 */
public class AmazonDynamoDBLocal {

    private static AmazonDynamoDB localAmazonDynamoDB;

    public static AmazonDynamoDB getAmazonDynamoDBLocal() {
        if (localAmazonDynamoDB == null) {
            return localAmazonDynamoDB = getNewAmazonDynamoDBLocal();
        } else {
            return localAmazonDynamoDB;
        }
    }

    public static AmazonDynamoDB getNewAmazonDynamoDBLocal() {
        System.setProperty("sqlite4java.library.path", "src/test/resources/bin");
        return DynamoDBEmbedded.create().amazonDynamoDB();
    }

}