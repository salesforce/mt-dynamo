package com.salesforce.dynamodbv2.dynamodblocal;

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
 * Tests LocalDynamoDbServer.
 *
 * @author msgroi
 */
@Tag("isolated-tests")
class LocalDynamoDbServerTest {

    @Test
    void fixedPorts() {
        int port1 = 8000;
        int port2 = port1 + 1;
        AmazonDynamoDB amazonDynamoDb = new LocalDynamoDbServer(port1).start();
        AmazonDynamoDB amazonDynamoDb2 = new LocalDynamoDbServer(port2).start();
        CreateTableRequest createTableRequest = new CreateTableRequest()
            .withAttributeDefinitions(new AttributeDefinition("hk", ScalarAttributeType.S))
            .withKeySchema(new KeySchemaElement("hk", KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        amazonDynamoDb.createTable(createTableRequest.withTableName("table" + port1));
        amazonDynamoDb2.createTable(createTableRequest.withTableName("table" + port2));
        amazonDynamoDb.listTables().getTableNames().forEach(table -> assertEquals("table" + port1, table));
        amazonDynamoDb2.listTables().getTableNames().forEach(table -> assertEquals("table" + port2, table));
    }

    @Test
    void randomPorts() {
        LocalDynamoDbServer dynamoDbServer = new LocalDynamoDbServer();
        LocalDynamoDbServer dynamoDbServer2 = new LocalDynamoDbServer();
        AmazonDynamoDB amazonDynamoDb = dynamoDbServer.start();
        AmazonDynamoDB amazonDynamoDb2 = dynamoDbServer2.start();
        CreateTableRequest createTableRequest = new CreateTableRequest()
            .withAttributeDefinitions(new AttributeDefinition("hk", ScalarAttributeType.S))
            .withKeySchema(new KeySchemaElement("hk", KeyType.HASH))
            .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        int port1 = dynamoDbServer.getPort();
        int port2 = dynamoDbServer2.getPort();
        amazonDynamoDb.createTable(createTableRequest.withTableName("table" + port1));
        amazonDynamoDb2.createTable(createTableRequest.withTableName("table" + port2));
        amazonDynamoDb.listTables().getTableNames().forEach(table -> assertEquals("table" + port1, table));
        amazonDynamoDb2.listTables().getTableNames().forEach(table -> assertEquals("table" + port2, table));
    }

}