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
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class AmazonDynamoDbLocal {

    private static AmazonDynamoDB localAmazonDynamoDb;
    private static AmazonDynamoDBStreams localAmazonDynamoDbStreams;

    public static AmazonDynamoDB getAmazonDynamoDbLocal() {
        initialize();
        return localAmazonDynamoDb;
    }

    public static AmazonDynamoDBStreams getAmazonDynamoDbStreamsLocal() {
        initialize();
        return localAmazonDynamoDbStreams;
    }

    private static void initialize() {
        if (localAmazonDynamoDb == null) {
            AmazonDynamoDBLocal amazonDynamoDbLocalClient = getNewAmazonDynamoDbLocalClient();
            localAmazonDynamoDb = amazonDynamoDbLocalClient.amazonDynamoDB();
            localAmazonDynamoDbStreams = amazonDynamoDbLocalClient.amazonDynamoDBStreams();
        }
    }

    public static AmazonDynamoDB getNewAmazonDynamoDbLocal() {
        return getNewAmazonDynamoDbLocalClient().amazonDynamoDB();
    }

    private static AmazonDynamoDBLocal getNewAmazonDynamoDbLocalClient() {
        System.setProperty("sqlite4java.library.path", "src/test/resources/bin");
        return DynamoDBEmbedded.create();
    }

}