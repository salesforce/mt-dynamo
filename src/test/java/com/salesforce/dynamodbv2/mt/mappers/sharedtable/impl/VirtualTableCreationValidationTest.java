package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.LSI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMappingFactory.VirtualTableCreationValidator;
import java.util.Map;
import org.junit.jupiter.api.Test;

class VirtualTableCreationValidationTest {

    private static final RandomPartitioningStrategy RANDOM_PARTITIONING_STRATEGY = new RandomPartitioningStrategy(S);
    private static final HashPartitioningStrategy HASH_PARTITIONING_STRATEGY = new HashPartitioningStrategy(10);

    @Test
    void validatePkCompatibility_randomPartitioning_valid() {
        RANDOM_PARTITIONING_STRATEGY.validateCompatiblePrimaryKeys(
            new PrimaryKey("hk", S, "rk", N), new PrimaryKey("hk", B, "rk", N));
    }

    @Test
    void validatePkCompatibility_randomPartitioning_missingVirtualHk() {
        assertException((TestFunction<NullPointerException>) () ->
                RANDOM_PARTITIONING_STRATEGY.validateCompatiblePrimaryKeys(
                    new PrimaryKey(null, S), new PrimaryKey("hk", N)),
            "hash key is required on virtual table");
    }

    @Test
    void validatePkCompatibility_randomPartitioning_missingPhysicalHk() {
        assertException((TestFunction<NullPointerException>) () ->
                RANDOM_PARTITIONING_STRATEGY.validateCompatiblePrimaryKeys(
                    new PrimaryKey("hk", S), new PrimaryKey(null, S)),
            "hash key is required on physical table");
    }

    @Test
    void validatePkCompatibility_randomPartitioning_invalidVirtualHkType() {
        assertException((TestFunction<IllegalArgumentException>) () ->
                RANDOM_PARTITIONING_STRATEGY.validateCompatiblePrimaryKeys(
                    new PrimaryKey("hk", S), new PrimaryKey("hk", N)),
            "hash key must be of type S");
    }

    @Test
    void validatePkCompatibility_randomPartitioning_physicalRkMissing() {
        assertException((TestFunction<IllegalArgumentException>) () ->
                RANDOM_PARTITIONING_STRATEGY.validateCompatiblePrimaryKeys(
                    new PrimaryKey("hk", S, "rk", S), new PrimaryKey("hk", S)),
            "rangeKey exists on virtual primary key but not on physical");
    }

    @Test
    void validatePkCompatibility_randomPartitioning_incompatibleRkTypes() {
        assertException((TestFunction<IllegalArgumentException>) () ->
                RANDOM_PARTITIONING_STRATEGY.validateCompatiblePrimaryKeys(
                    new PrimaryKey("hk", S, "rk", S), new PrimaryKey("hk", S, "rk", N)),
            "virtual and physical range-key types mismatch");
    }

    @Test
    void validatePkCompatibility_hashPartitioning_valid() {
        HASH_PARTITIONING_STRATEGY.validateCompatiblePrimaryKeys(
            new PrimaryKey("hk", S), new PrimaryKey("hk", B, "rk", B));
    }

    @Test
    void validatePkCompatibility_hashPartitioning_invalidPhysicalPk() {
        assertException((TestFunction<IllegalArgumentException>) () ->
                HASH_PARTITIONING_STRATEGY.validateCompatiblePrimaryKeys(
                    new PrimaryKey("hk", S),
                    new PrimaryKey("hk", B)),
            "physical primary key must have hash type B and range type B");
    }

    @Test
    void validatePhysicalTable_randomPartitioning() {
        VirtualTableCreationValidator validator = new VirtualTableCreationValidator(RANDOM_PARTITIONING_STRATEGY);
        assertException((TestFunction<IllegalArgumentException>) () ->
                validator.validatePhysicalTable(TableMappingTestUtil.buildTable(
                    "physicalTableName",
                    new PrimaryKey("physicalHk", N))),
            "physical table physicalTableName has invalid primary key");
        assertException((TestFunction<IllegalArgumentException>) () ->
                validator.validatePhysicalTable(TableMappingTestUtil.buildTable(
                    "physicalTableName",
                    new PrimaryKey("physicalHk", S),
                    ImmutableMap.of("physicalGsi", new PrimaryKey("physicalGsiHk", N)))),
            "physical table physicalTableName's GSI physicalGsi has invalid primary key");
    }

    @Test
    void validatePhysicalTable_hashPartitioning() {
        VirtualTableCreationValidator validator = new VirtualTableCreationValidator(HASH_PARTITIONING_STRATEGY);
        assertException((TestFunction<IllegalArgumentException>) () ->
                validator.validatePhysicalTable(TableMappingTestUtil.buildTable(
                    "physicalTableName",
                    new PrimaryKey("physicalHk", S))),
            "physical table physicalTableName has invalid primary key");
        assertException((TestFunction<IllegalArgumentException>) () ->
                validator.validatePhysicalTable(TableMappingTestUtil.buildTable(
                    "physicalTableName",
                    new PrimaryKey("physicalHk", B, "physicalRk", B),
                    ImmutableMap.of("physicalGsi", new PrimaryKey("physicalGsiHk", B)))),
            "physical table physicalTableName's GSI physicalGsi has invalid primary key");
    }

    @Test
    void validateSecondaryIndexMapping() {
        VirtualTableCreationValidator validator = new VirtualTableCreationValidator(HASH_PARTITIONING_STRATEGY);
        DynamoTableDescription virtualTable = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder.builder()
                .withTableName("virtualTableName")
                .withTableKeySchema("virtualHk", S, "virtualRk", N)
                .addSi("virtualGsi1", GSI, new PrimaryKey("virtualGsi1Hk", S, "virtualGsi1Rk", N), 1L)
                .addSi("virtualLsi", LSI, new PrimaryKey("virtualLsiHk", S, "virtualLsiRk", N), 1L)
                .addSi("virtualGsi2", GSI, new PrimaryKey("virtualGsi2Hk", S), 1L)
                .build());
        DynamoTableDescription physicalTable = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder
                .builder()
                .withTableName("physicalTableName")
                .withTableKeySchema("physicalHk", B, "physicalRk", B)
                .addSi("physicalGsi1", GSI, new PrimaryKey("physicalGsi1Hk", B, "physicalGsi1Rk", B), 1L)
                .addSi("physicalGsi2", GSI, new PrimaryKey("physicalGsi2Hk", B, "physicalGsi2Rk", B), 1L)
                .addSi("physicalLsi", LSI, new PrimaryKey("physicalLsiHk", B, "physicalLsiRk", B), 1L)
                .build());
        Map<DynamoSecondaryIndex, DynamoSecondaryIndex> expected = ImmutableMap.of(
            virtualTable.findSi("virtualGsi1"), physicalTable.findSi("physicalGsi1"),
            virtualTable.findSi("virtualGsi2"), physicalTable.findSi("physicalGsi2"),
            virtualTable.findSi("virtualLsi"), physicalTable.findSi("physicalLsi")
        );
        assertEquals(expected, validator.validateAndGetSecondaryIndexMap(virtualTable, physicalTable));
    }

    @Test
    void validateSecondaryIndexes_outOfIndexes() {
        VirtualTableCreationValidator validator = new VirtualTableCreationValidator(HASH_PARTITIONING_STRATEGY);
        DynamoTableDescription virtualTable = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder.builder()
                .withTableName("virtualTableName")
                .withTableKeySchema("virtualHk", S, "virtualRk", N)
                .addSi("virtualGsi1", GSI, new PrimaryKey("virtualGsi1Hk", S, "virtualGsi1Rk", N), 1L)
                .addSi("virtualGsi2", GSI, new PrimaryKey("virtualGsi2Hk", S, "virtualGsi2Rk", N), 1L)
                .build());
        DynamoTableDescription physicalTable = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder
                .builder()
                .withTableName("physicalTableName")
                .withTableKeySchema("physicalHk", B, "physicalRk", B)
                .addSi("physicalGsi1", GSI, new PrimaryKey("physicalGsi1Hk", B, "physicalGsi1Rk", B), 1L)
                .build());
        assertException((TestFunction<IllegalArgumentException>) () ->
                validator.validateAndGetSecondaryIndexMap(virtualTable, physicalTable),
            "failure mapping virtual GSI");
    }

    @Test
    void validateSecondaryIndexes_incompatibleIndexTypes() {
        VirtualTableCreationValidator validator = new VirtualTableCreationValidator(HASH_PARTITIONING_STRATEGY);
        DynamoTableDescription virtualTable = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder.builder()
                .withTableName("virtualTableName")
                .withTableKeySchema("virtualHk", S, "virtualRk", N)
                .addSi("virtualLsi", LSI, new PrimaryKey("virtualLsiHk", B, "virtualLsiRk", B), 1L)
                .build());
        DynamoTableDescription physicalTable = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder
                .builder()
                .withTableName("physicalTableName")
                .withTableKeySchema("physicalHk", B, "physicalRk", B)
                .addSi("physicalGsi", GSI, new PrimaryKey("physicalGsiHk", B, "physicalGsiRk", B), 1L)
                .build());
        assertException((TestFunction<IllegalArgumentException>) () ->
                validator.validateAndGetSecondaryIndexMap(virtualTable, physicalTable),
            "failure mapping virtual LSI");
    }

    @Test
    void validateSecondaryIndexes_incompatiblePrimaryKey() {
        VirtualTableCreationValidator validator = new VirtualTableCreationValidator(HASH_PARTITIONING_STRATEGY);
        DynamoTableDescription virtualTable = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder.builder()
                .withTableName("virtualTableName")
                .withTableKeySchema("virtualHk", S, "virtualRk", N)
                .addSi("virtualGsi1", GSI, new PrimaryKey("virtualGsi1Hk", S, "virtualGsi1Rk", N), 1L)
                .build());
        DynamoTableDescription physicalTable = new DynamoTableDescriptionImpl(
            CreateTableRequestBuilder
                .builder()
                .withTableName("physicalTableName")
                .withTableKeySchema("physicalHk", B, "physicalRk", B)
                .addSi("physicalGsi1", GSI, new PrimaryKey("physicalGsi1Hk", B), 1L)
                .build());
        assertException((TestFunction<IllegalArgumentException>) () ->
                validator.validateAndGetSecondaryIndexMap(virtualTable, physicalTable),
            "failure mapping virtual GSI");
    }

    private static void assertException(TestFunction<? extends Throwable> test, String expectedContainedMessage) {
        try {
            test.run();
            fail("Expected exception '" + expectedContainedMessage + "' not encountered");
        } catch (IllegalArgumentException | IllegalStateException | NullPointerException e) {
            assertTrue(e.getMessage().contains(expectedContainedMessage),
                "expectedContainedMessage=" + expectedContainedMessage + ", actual=" + e.getMessage());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private interface TestFunction<T extends Throwable> {

        void run() throws T;
    }
}
