package com.salesforce.dynamodbv2.testsupport;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;

/**
 * Implement this interface to provide your own test set up implementation.
 *
 * @author msgroi
 */
public interface TestSetup {

    void setupTest(TestArgument testArgument);

    void setupTableData(AmazonDynamoDB amazonDynamoDb,
        ScalarAttributeType hashKeyAttrType,
        String org,
        CreateTableRequest createTableRequest);

}