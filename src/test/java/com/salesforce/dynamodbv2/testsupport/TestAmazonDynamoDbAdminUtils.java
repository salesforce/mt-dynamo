/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.testsupport;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.salesforce.dynamodbv2.mt.admin.AmazonDynamoDbAdminUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public class TestAmazonDynamoDbAdminUtils extends AmazonDynamoDbAdminUtils {

    public TestAmazonDynamoDbAdminUtils(AmazonDynamoDB amazonDynamoDb) {
        super(amazonDynamoDb);
    }

    /**
     * TODO: write Javadoc.
     */
    public static String getLocalHost() {
        try {
            String fqname = InetAddress.getLocalHost().getHostName();
            int firstDot = fqname.indexOf('.');
            return firstDot == -1 ? fqname : fqname.substring(0, firstDot);
        } catch (UnknownHostException e) {
            throw new RuntimeException("could not get hostname", e);
        }
    }

}