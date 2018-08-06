package com.salesforce.dynamodbv2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author msgroi
 */
@Tag("isolated-tests")
class LocalDynamoDBServerTest {

    @Test
    void fixedPorts() {
        int port1 = 8000;
        int port2 = port1 + 1;
        AmazonDynamoDB amazonDynamoDB = new LocalDynamoDBServer(port1).start();
        AmazonDynamoDB amazonDynamoDB2 = new LocalDynamoDBServer(port2).start();
        CreateTableRequest createTableRequest = new CreateTableRequest()
            .withAttributeDefinitions(new AttributeDefinition("hk", ScalarAttributeType.S))
            .withKeySchema(new KeySchemaElement("hk", KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        amazonDynamoDB.createTable(createTableRequest.withTableName("table" + port1));
        amazonDynamoDB2.createTable(createTableRequest.withTableName("table" + port2));
        amazonDynamoDB.listTables().getTableNames().forEach(table -> assertEquals("table" + port1, table));
        amazonDynamoDB2.listTables().getTableNames().forEach(table -> assertEquals("table" + port2, table));
    }

    @Test
    void randomPorts() {
        LocalDynamoDBServer dynamoDBServer = new LocalDynamoDBServer();
        LocalDynamoDBServer dynamoDBServer2 = new LocalDynamoDBServer();
        AmazonDynamoDB amazonDynamoDB = dynamoDBServer.start();
        AmazonDynamoDB amazonDynamoDB2 = dynamoDBServer2.start();
        CreateTableRequest createTableRequest = new CreateTableRequest()
            .withAttributeDefinitions(new AttributeDefinition("hk", ScalarAttributeType.S))
            .withKeySchema(new KeySchemaElement("hk", KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        int port1 = dynamoDBServer.getPort();
        int port2 = dynamoDBServer2.getPort();
        amazonDynamoDB.createTable(createTableRequest.withTableName("table" + port1));
        amazonDynamoDB2.createTable(createTableRequest.withTableName("table" + port2));
        amazonDynamoDB.listTables().getTableNames().forEach(table -> assertEquals("table" + port1, table));
        amazonDynamoDB2.listTables().getTableNames().forEach(table -> assertEquals("table" + port2, table));
    }

}