/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_GSI_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_GSI_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.PHYSICAL_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.SOME_FIELD;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_GSI;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_GSI_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_GSI_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_HK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.VIRTUAL_RK;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.buildVirtualHkRkTable;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.buildVirtualHkTable;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getPhysicalHkValue;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getPhysicalRkValue;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.getTableMapping;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.HashPartitioningTestUtil.TestHashKeyValue;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class HashPartitioningItemMapperTest {

    private static final ScalarAttributeType VIRTUAL_RK_TYPE = N;
    private static final ScalarAttributeType VIRTUAL_GSI_RK_TYPE = B;
    private static final AttributeValue VIRTUAL_RK_VALUE = new AttributeValue().withN("-9.87");
    private static final AttributeValue VIRTUAL_GSI_RK_VALUE = new AttributeValue().withB(
        ByteBuffer.wrap(new byte[]{6, 7, 8}));
    private static final AttributeValue SOME_VALUE = new AttributeValue().withS("someValue");

    private static TestHashKeyValue getVirtualHkValue(ScalarAttributeType type, boolean forGsi) {
        switch (type) {
            case S:
                return new TestHashKeyValue(forGsi ? "gsiHkValue" : "hkValue");
            case N:
                return new TestHashKeyValue(forGsi ? new BigDecimal("-654.321") : new BigDecimal("123.456"));
            case B:
                return new TestHashKeyValue(forGsi ? new byte[]{-1, -2} : new byte[]{3, 4, 5});
            default:
                throw new IllegalArgumentException("Invalid hash key type: " + type);
        }
    }

    @ParameterizedTest
    @EnumSource(value = ScalarAttributeType.class, names = {"S", "B", "N"})
    void applyAndReverse_hkTable_noGsiValue(ScalarAttributeType hkType) {
        runApplyAndReverseTest(hkType, false /*iskRkTable*/, false /*hasGsiHk*/, false /*hasGsiRk*/);
    }

    @ParameterizedTest
    @EnumSource(value = ScalarAttributeType.class, names = {"S", "B", "N"})
    void applyAndReverse_hkTable_withGsiValue(ScalarAttributeType hkType) {
        runApplyAndReverseTest(hkType, false /*iskRkTable*/, true /*hasGsiHk*/, false /*hasGsiRk*/);
    }

    @ParameterizedTest
    @EnumSource(value = ScalarAttributeType.class, names = {"S", "B", "N"})
    void applyAndReverse_hkRkTable_noGsiValues(ScalarAttributeType hkType) {
        runApplyAndReverseTest(hkType, true /*iskRkTable*/, false /*hasGsiHk*/, false /*hasGsiRk*/);
    }

    @ParameterizedTest
    @EnumSource(value = ScalarAttributeType.class, names = {"S", "B", "N"})
    void applyAndReverse_hkRkTable_withGsiValues(ScalarAttributeType hkType) {
        runApplyAndReverseTest(hkType, true /*iskRkTable*/, true /*hasGsiHk*/, true /*hasGsiRk*/);
    }

    @ParameterizedTest
    @EnumSource(value = ScalarAttributeType.class, names = {"S", "B", "N"})
    void applyAndReverse_hkRkTable_notAllGsiValues(ScalarAttributeType hkType) {
        runApplyAndReverseTest(hkType, true /*iskRkTable*/, true /*hasGsiHk*/, false /*hasGsiRk*/);
    }

    private void runApplyAndReverseTest(ScalarAttributeType hkType, boolean isHkRkTable, boolean hasGsiHk,
                                        boolean hasGsiRk) {
        TestHashKeyValue virtualHkValue = getVirtualHkValue(hkType, false);
        TestHashKeyValue virtualGsiHkValue = getVirtualHkValue(hkType, true);

        Map<String, AttributeValue> item = new HashMap<>();
        item.put(VIRTUAL_HK, virtualHkValue.getAttributeValue());
        item.put(SOME_FIELD, SOME_VALUE);
        if (isHkRkTable) {
            item.put(VIRTUAL_RK, VIRTUAL_RK_VALUE);
        }
        if (hasGsiHk) {
            item.put(VIRTUAL_GSI_HK, virtualGsiHkValue.getAttributeValue());
            if (hasGsiRk) {
                item.put(VIRTUAL_GSI_RK, VIRTUAL_GSI_RK_VALUE);
            }
        }

        Map<String, AttributeValue> expectedMappedItem = new HashMap<>(item);
        expectedMappedItem.put(PHYSICAL_HK, getPhysicalHkValue(virtualHkValue));
        expectedMappedItem.put(PHYSICAL_RK, isHkRkTable
            ? getPhysicalRkValue(hkType, virtualHkValue.getAttributeValue(), N, VIRTUAL_RK_VALUE)
            : HashPartitioningTestUtil.getPhysicalRkValue(hkType, virtualHkValue.getAttributeValue()));
        if (isHkRkTable && hasGsiHk && hasGsiRk) {
            expectedMappedItem.put(PHYSICAL_GSI_HK, getPhysicalHkValue(virtualGsiHkValue));
            expectedMappedItem.put(PHYSICAL_GSI_RK,
                getPhysicalRkValue(hkType, virtualGsiHkValue.getAttributeValue(), B, VIRTUAL_GSI_RK_VALUE));
        } else if (!isHkRkTable && hasGsiHk) {
            expectedMappedItem.put(PHYSICAL_GSI_HK, getPhysicalHkValue(virtualGsiHkValue));
            expectedMappedItem.put(PHYSICAL_GSI_RK,
                HashPartitioningTestUtil.getPhysicalRkValue(hkType, virtualGsiHkValue.getAttributeValue()));
        }

        DynamoTableDescription virtualTable = isHkRkTable
            ? buildVirtualHkRkTable(hkType, VIRTUAL_RK_TYPE, hkType, VIRTUAL_GSI_RK_TYPE)
            : buildVirtualHkTable(hkType);
        HashPartitioningItemMapper itemMapper = getTableMapping(virtualTable).getItemMapper();

        Map<String, AttributeValue> mappedItem = itemMapper.applyForWrite(item);
        assertEquals(expectedMappedItem, mappedItem);

        Map<String, AttributeValue> reversedItem = itemMapper.reverse(mappedItem);
        assertEquals(item, reversedItem);
    }

    @ParameterizedTest
    @EnumSource(value = ScalarAttributeType.class, names = {"S", "B", "N"})
    void applyToKeyAttributes_hkTable(ScalarAttributeType hkType) {
        runApplyToKeyAttributesTest(hkType, false /*isHkRkTable*/, false /*onGsi*/);
    }

    @ParameterizedTest
    @EnumSource(value = ScalarAttributeType.class, names = {"S", "B", "N"})
    void applyToKeyAttributes_gsi_hkTable(ScalarAttributeType hkType) {
        runApplyToKeyAttributesTest(hkType, false /*isHkRkTable*/, true /*onGsi*/);
    }

    @ParameterizedTest
    @EnumSource(value = ScalarAttributeType.class, names = {"S", "B", "N"})
    void applyToKeyAttributes_hkRkTable(ScalarAttributeType hkType) {
        runApplyToKeyAttributesTest(hkType, true /*isHkRkTable*/, false /*onGsi*/);
    }

    @ParameterizedTest
    @EnumSource(value = ScalarAttributeType.class, names = {"S", "B", "N"})
    void applyToKeyAttributes_gsi_hkRkTable(ScalarAttributeType hkType) {
        runApplyToKeyAttributesTest(hkType, true /*isHkRkTable*/, true /*onGsi*/);
    }

    private void runApplyToKeyAttributesTest(ScalarAttributeType hkType, boolean isHkRkTable, boolean onGsi) {
        TestHashKeyValue virtualHkValue = getVirtualHkValue(hkType, false);
        TestHashKeyValue virtualGsiHkValue = getVirtualHkValue(hkType, true);

        Map<String, AttributeValue> item = new HashMap<>();
        item.put(VIRTUAL_HK, virtualHkValue.getAttributeValue());
        item.put(VIRTUAL_GSI_HK, virtualGsiHkValue.getAttributeValue());
        item.put(SOME_FIELD, SOME_VALUE);
        if (isHkRkTable) {
            item.put(VIRTUAL_RK, VIRTUAL_RK_VALUE);
            item.put(VIRTUAL_GSI_RK, VIRTUAL_GSI_RK_VALUE);
        }

        Map<String, AttributeValue> expectedMappedItem = new HashMap<>();
        expectedMappedItem.put(PHYSICAL_HK, getPhysicalHkValue(virtualHkValue));
        expectedMappedItem.put(PHYSICAL_RK, isHkRkTable
            ? getPhysicalRkValue(hkType, virtualHkValue.getAttributeValue(), N, VIRTUAL_RK_VALUE)
            : HashPartitioningTestUtil.getPhysicalRkValue(hkType, virtualHkValue.getAttributeValue()));
        if (onGsi) {
            expectedMappedItem.put(PHYSICAL_GSI_HK, getPhysicalHkValue(virtualGsiHkValue));
            expectedMappedItem.put(PHYSICAL_GSI_RK, isHkRkTable
                ? getPhysicalRkValue(hkType, virtualGsiHkValue.getAttributeValue(), B, VIRTUAL_GSI_RK_VALUE)
                : HashPartitioningTestUtil.getPhysicalRkValue(hkType, virtualGsiHkValue.getAttributeValue()));
        }

        DynamoTableDescription virtualTable = isHkRkTable
            ? buildVirtualHkRkTable(hkType, VIRTUAL_RK_TYPE, hkType, VIRTUAL_GSI_RK_TYPE)
            : buildVirtualHkTable(hkType);
        HashPartitioningTableMapping tableMapping = getTableMapping(virtualTable);
        HashPartitioningItemMapper itemMapper = tableMapping.getItemMapper();

        DynamoSecondaryIndex virtualSi = onGsi ? virtualTable.findSi(VIRTUAL_GSI) : null;
        Map<String, AttributeValue> mappedItem = itemMapper.applyToKeyAttributes(item, virtualSi);
        assertEquals(expectedMappedItem, mappedItem);
    }

    @Test
    void reverseNull() {
        HashPartitioningItemMapper itemMapper = getTableMapping(buildVirtualHkTable(S)).getItemMapper();
        assertNull(itemMapper.reverse(null));
    }

}
