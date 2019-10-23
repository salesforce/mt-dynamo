package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.TableMappingTestUtil.buildTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Identity;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RandomPartitioningRecordMapperTest {

    private final String context = "context";
    private final String tableName = "table";
    private final MtAmazonDynamoDbContextProvider provider = () -> Optional.of(context);
    private final String virtualHk = "vhk";
    private final String virtualRk = "vrk";
    private final String physicalHk = "phk";
    private final String physicalRk = "prk";
    private final RandomPartitioningTableMapping tableMapping = new RandomPartitioningTableMapping(
        buildTable(tableName, new PrimaryKey(virtualHk, S, virtualRk, S)),
        buildTable("physicalTable", new PrimaryKey(physicalHk, S, physicalRk, S)),
        index -> null,
        provider
    );
    private final RecordMapper sut = tableMapping.getRecordMapper();

    @ParameterizedTest
    @ValueSource(strings = { "context/table/abc", "context/table/0" })
    void testFilter(String qualifiedHashKey) {
        final Record record = new Record().withDynamodb(new StreamRecord().withKeys(ImmutableMap.of(
            physicalHk, new AttributeValue(qualifiedHashKey), physicalRk, new AttributeValue("val")
        )));
        assertTrue(sut.createFilter().test(record));
    }

    @ParameterizedTest
    @ValueSource(strings = { "context/table2/abc", "context2/table/0" })
    void testFilterNegative(String qualifiedHashKey) {
        final Record record = new Record().withDynamodb(new StreamRecord().withKeys(ImmutableMap.of(
            physicalHk, new AttributeValue(qualifiedHashKey), physicalRk, new AttributeValue("val")
        )));
        assertFalse(sut.createFilter().test(record));
    }

    @Test
    void testMap() {
        final AttributeValue physicalHkVal = new AttributeValue("context/table/hkVal");
        final AttributeValue physicalRkVal = new AttributeValue("rkVal");
        final AttributeValue virtualHkVal = new AttributeValue("hkVal");
        final AttributeValue virtualRkVal = physicalRkVal.clone();

        final String attrName = "someField";
        final AttributeValue oldAttrVal = new AttributeValue("old");
        final AttributeValue newAttrVal = new AttributeValue("new");

        final Record record = new Record()
            .withEventID("e1")
            .withEventName(OperationType.INSERT)
            .withAwsRegion(Regions.US_EAST_1.getName())
            .withEventSource("aws:dynamodb")
            .withEventVersion("1.0")
            .withUserIdentity(new Identity().withPrincipalId("p").withType("Service"))
            .withDynamodb(
                new StreamRecord()
                    .withKeys(ImmutableMap.of(physicalHk, physicalHkVal, physicalRk, physicalRkVal))
                    .withOldImage(ImmutableMap.of(physicalHk, physicalHkVal, physicalRk, physicalRkVal,
                        attrName, oldAttrVal))
                    .withNewImage(ImmutableMap.of(physicalHk, physicalHkVal, physicalRk, physicalRkVal,
                        attrName, newAttrVal))
                    .withSequenceNumber("1234")
                    .withSizeBytes(10L)
                    .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
            );
        final Record expected = record.clone();
        expected.withDynamodb(expected.getDynamodb().clone());
        expected.getDynamodb()
            .withKeys(ImmutableMap.of(virtualHk, virtualHkVal, virtualRk, virtualRkVal))
            .withOldImage(ImmutableMap.of(virtualHk, virtualHkVal, virtualRk, virtualRkVal, attrName, oldAttrVal))
            .withNewImage(ImmutableMap.of(virtualHk, virtualHkVal, virtualRk, virtualRkVal, attrName, newAttrVal));

        assertEquals(expected, sut.apply(record));
    }

}
