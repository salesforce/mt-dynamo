package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for specific MtAmazonDynamoDbBySharedTable methods.
 *
 * @author msgroi
 */
class MtAmazonDynamoDbBySharedTableTest {

    @Test
    void projectionContainsKey_nullProject() {
        assertTrue(MtAmazonDynamoDbBySharedTable.projectionContainsKey(new ScanRequest(), null));
    }

    @Test
    void projectionContainsKey_hkInProjection() {
        assertTrue(MtAmazonDynamoDbBySharedTable.projectionContainsKey(
            new ScanRequest().withProjectionExpression("hk"), new PrimaryKey("hk", S)));
    }

    @Test
    void projectionContainsKey_hkInExpressionAttrNames() {
        assertTrue(MtAmazonDynamoDbBySharedTable.projectionContainsKey(
            new ScanRequest().withProjectionExpression("value")
                .withExpressionAttributeNames(ImmutableMap.of("hk", "value")),
            new PrimaryKey("hk", S)));
    }

    @Test
    void projectionContainsKey_hkInLegacyProjection() {
        assertTrue(MtAmazonDynamoDbBySharedTable.projectionContainsKey(
            new ScanRequest().withAttributesToGet("hk"), new PrimaryKey("hk", S)));
    }

}