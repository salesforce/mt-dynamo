package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;
import static java.util.Optional.empty;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CreateTableRequestBuilderTest {

    private static final String HASH_KEY_FIELD = "hk";
    private static final String RANGE_KEY_FIELD = "rk";
    private PrimaryKey primaryKey;
    private String indexName;
    private DynamoSecondaryIndex.DynamoSecondaryIndexType indexType;

    private CreateTableRequestBuilder testBuilder;

    void assertGlobalSecondaryIndexProvisionedThroughputResults(GlobalSecondaryIndex gsi,
                                                                Long expectedProvisionedThroughput) {
        assertNotNull(gsi.getProvisionedThroughput());
        assert (gsi.getProvisionedThroughput().getReadCapacityUnits().equals(expectedProvisionedThroughput));
        assert (gsi.getProvisionedThroughput().getWriteCapacityUnits().equals(expectedProvisionedThroughput));
    }

    @BeforeEach
    void beforeEach() {

        testBuilder = CreateTableRequestBuilder.builder()
                .withTableName("testBuilder")
                .withTableKeySchema(HASH_KEY_FIELD, S, RANGE_KEY_FIELD, S);

        indexType = GSI;
        ScalarAttributeType hashKeyType = S;
        Optional<ScalarAttributeType> rangeKeyType = empty();

        indexName = indexType.name().toLowerCase() + "_"
                + hashKeyType.name().toLowerCase()
                + rangeKeyType.map(type -> "_" + type.name().toLowerCase()).orElse("").toLowerCase();
        primaryKey = rangeKeyType.map(
            scalarAttributeType -> new PrimaryKey(indexType == LSI ? "hk" : indexName + "_hk",
                    hashKeyType,
                    indexName + "_rk",
                    scalarAttributeType))
            .orElseGet(() -> new PrimaryKey(indexName + "_hk",
                    hashKeyType));
    }

    @Test
    void testGlobalSecondaryIndexProvisionedThroughputIsNullForPayPerRequest() {
        testBuilder.withBillingMode(BillingMode.PAY_PER_REQUEST);
        testBuilder.addSi(indexName, indexType, primaryKey, 5L);

        for (GlobalSecondaryIndex gsi: testBuilder.getCreateTableRequest().getGlobalSecondaryIndexes()) {
            assertNull(gsi.getProvisionedThroughput());
        }
    }

    @Test
    void testGlobalSecondaryIndexProvisionedThroughputIsSetForNullBillingMode() {
        testBuilder.withBillingMode(null);
        testBuilder.addSi(indexName, indexType, primaryKey, 5L);

        for (GlobalSecondaryIndex gsi: testBuilder.getCreateTableRequest().getGlobalSecondaryIndexes()) {
            assertGlobalSecondaryIndexProvisionedThroughputResults(gsi, 5L);
        }
    }

    @Test
    void testGlobalSecondaryIndexProvisionedThroughputIsSetForProvisioned() {
        testBuilder.withBillingMode(BillingMode.PROVISIONED);
        testBuilder.addSi(indexName, indexType, primaryKey, 5L);

        for (GlobalSecondaryIndex gsi: testBuilder.getCreateTableRequest().getGlobalSecondaryIndexes()) {
            assertGlobalSecondaryIndexProvisionedThroughputResults(gsi, 5L);
        }
    }
}