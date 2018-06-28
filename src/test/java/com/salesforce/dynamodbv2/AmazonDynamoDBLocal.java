/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;

/**
 * @author msgroi
 */
public class AmazonDynamoDBLocal {

    private static AmazonDynamoDB localAmazonDynamoDB;
    private static AmazonDynamoDBStreams localAmazonDynamoDBStreams;

    public static AmazonDynamoDB getAmazonDynamoDBLocal() {
        initialize();
        return localAmazonDynamoDB;
    }

    public static AmazonDynamoDBStreams getAmazonDynamoDBStreamsLocal() {
        initialize();
        return localAmazonDynamoDBStreams;
    }

    private static void initialize() {
        if (localAmazonDynamoDB == null) {
            com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal amazonDynamoDBLocalClient = getNewAmazonDynamoDBLocalClient();
            localAmazonDynamoDB = amazonDynamoDBLocalClient.amazonDynamoDB();
            localAmazonDynamoDBStreams = amazonDynamoDBLocalClient.amazonDynamoDBStreams();
        }
    }

    public static AmazonDynamoDB getNewAmazonDynamoDBLocal() {
        return getNewAmazonDynamoDBLocalClient().amazonDynamoDB();
    }

    private static com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal getNewAmazonDynamoDBLocalClient() {
        System.setProperty("sqlite4java.library.path", "src/test/resources/bin");
        return DynamoDBEmbedded.create();
    }

}