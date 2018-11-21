package com.salesforce.dynamodbv2.mt.repo;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl;
import com.salesforce.dynamodbv2.mt.repo.MtDynamoDbTableDescriptionRepo.MtDynamoDbTableDescriptionRepoBuilder;
import org.junit.jupiter.api.Test;

public class MtDynamoDbTableDescriptionRepoTest {

    /**
     * Verifies that changing provisioned throughput on the metadata table doesn't cause comparison to fail on restart.
     */
    @Test
    void testMetadataTableProvisioningThroughputChange() {
        AmazonDynamoDB dynamoDb = AmazonDynamoDbLocal.getAmazonDynamoDbLocal();
        MtAmazonDynamoDbContextProvider ctx = new MtAmazonDynamoDbContextProviderImpl();
        String tableName = "MtDynamoDbTableDescriptionRepoTest_testMetadataTableExists_metadata";

        MtDynamoDbTableDescriptionRepoBuilder b = MtDynamoDbTableDescriptionRepo.builder()
            .withAmazonDynamoDb(dynamoDb)
            .withContext(ctx)
            .withTableDescriptionTableName(tableName);

        MtDynamoDbTableDescriptionRepo repo = b.build();
        ctx.withContext("1", () ->
            repo.createTable(new CreateTableRequest()
                .withTableName("test")
                .withKeySchema(new KeySchemaElement("id", KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)))
        );

        dynamoDb.updateTable(new UpdateTableRequest(tableName, new ProvisionedThroughput(6L, 6L)));

        MtDynamoDbTableDescriptionRepo repo2 = b.build();
        ctx.withContext("1", () ->
            assertNotNull(repo2.getTableDescription("test"))
        );
    }
}
