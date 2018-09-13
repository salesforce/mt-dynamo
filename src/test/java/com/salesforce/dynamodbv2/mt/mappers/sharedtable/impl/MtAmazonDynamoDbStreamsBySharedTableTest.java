package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.amazonaws.services.dynamodbv2.model.StreamViewType.KEYS_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbStreams;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests shared table streams.
 */
class MtAmazonDynamoDbStreamsBySharedTableTest {

    /**
     * Test utility method.
     */
    private static CreateTableRequest newCreateTableRequest(String tableName) {
        return new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(new KeySchemaElement("id", HASH))
                .withAttributeDefinitions(new AttributeDefinition("id", S))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))
                .withStreamSpecification(new StreamSpecification()
                        .withStreamEnabled(true)
                        .withStreamViewType(KEYS_ONLY));
    }

    /**
     * Verifies that list streams returns only streams for mt tables for the given client.
     */
    @Test
    void testListStreams() {
        String prefix = MtAmazonDynamoDbStreamsBySharedTable.class.getSimpleName() + ".";
        String sharedTableName = "SharedTable";
        String randomTableName = "RandomTable";
        AmazonDynamoDB dynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();

        MtAmazonDynamoDbBySharedTable mtDynamoDb = SharedTableBuilder.builder()
                .withCreateTableRequests(newCreateTableRequest(sharedTableName))
                .withAmazonDynamoDb(dynamoDb)
                .withTablePrefix(prefix)
                .withPrecreateTables(true)
                .withContext(() -> null)
                .build();
        try {
            TableUtils.createTableIfNotExists(dynamoDb, newCreateTableRequest(randomTableName));

            AmazonDynamoDBStreams dynamoDbStreams = AmazonDynamoDbLocal.getAmazonDynamoDbStreamsLocal();
            MtAmazonDynamoDbStreams mtDynamoDbStreams = MtAmazonDynamoDbStreams.createFromDynamo(mtDynamoDb,
                    dynamoDbStreams);

            List<Stream> streams = mtDynamoDbStreams.listStreams(new ListStreamsRequest()).getStreams();
            assertEquals(1, streams.size());

            Stream stream = streams.get(0);
            assertEquals(prefix + sharedTableName, stream.getTableName());
            assertNotNull(stream.getStreamArn());
            assertNotNull(stream.getStreamLabel());
        } finally {
            mtDynamoDb.getSharedTables().stream()
                    .map(CreateTableRequest::getTableName)
                    .forEach(name -> TableUtils.deleteTableIfExists(dynamoDb, new DeleteTableRequest(name)));
            TableUtils.deleteTableIfExists(dynamoDb, new DeleteTableRequest(randomTableName));
        }
    }

}
