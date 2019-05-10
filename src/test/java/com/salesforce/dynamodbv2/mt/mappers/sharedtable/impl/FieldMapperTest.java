/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.SECONDARY_INDEX;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class FieldMapperTest {

    private static final char DELIMITER = '/';
    private static final String CONTEXT = "context";
    private static final String TABLE_NAME = "table";
    private static final String PREFIX = CONTEXT + DELIMITER + TABLE_NAME + DELIMITER;
    private static final MtAmazonDynamoDbContextProvider CONTEXT_PROVIDER = () -> Optional.of(CONTEXT);
    private static final FieldMapper SFM = new StringFieldMapper(CONTEXT_PROVIDER, TABLE_NAME);
    private static final FieldMapper BFM = new BinaryFieldMapper(CONTEXT_PROVIDER, TABLE_NAME);
    private static final byte[] TEST_BYTES = { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 };

    private static ByteBuffer prefix(int valueLength) {
        return ByteBuffer.allocate(CONTEXT.length() + TABLE_NAME.length() + 2 + valueLength)
            .put(UTF_8.encode(CONTEXT)).put((byte) 0x00).put(UTF_8.encode(TABLE_NAME)).put((byte) 0x00);
    }

    static Stream<Object[]> data() {
        return Arrays.stream(new Object[][] {
            { SFM, S, S, TABLE, new AttributeValue("value"), new AttributeValue(PREFIX + "value") },
            { SFM, S, S, SECONDARY_INDEX, new AttributeValue("a/b"), new AttributeValue(PREFIX + "a/b") },
            { SFM, N, S, TABLE, new AttributeValue().withN("123"), new AttributeValue(PREFIX + "123") },
            { SFM, B, S, TABLE, new AttributeValue().withB(ByteBuffer.wrap(TEST_BYTES)),
                new AttributeValue(PREFIX + Base64.getEncoder().encodeToString(TEST_BYTES)) },
            { BFM, S, B, TABLE, new AttributeValue("value"), new AttributeValue()
                .withB(prefix(5).put(UTF_8.encode("value")).flip()) },
            { BFM, S, B, SECONDARY_INDEX, new AttributeValue("a\u0000b"), new AttributeValue()
                .withB(prefix(3).put(UTF_8.encode("a\u0000b")).flip()) },
            { BFM, N, B, TABLE, new AttributeValue().withN("1.1"), new AttributeValue().withB(
                prefix(5).put(ByteBuffer.allocate(5).putInt(1).put((byte) 11).array()).flip()) },
            { BFM, B, B, TABLE, new AttributeValue().withB(ByteBuffer.wrap(TEST_BYTES)),
                new AttributeValue().withB(prefix(TEST_BYTES.length).put(TEST_BYTES).flip())}
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    void test(FieldMapper fieldMapper,
              ScalarAttributeType virtualFieldType,
              ScalarAttributeType physicalFieldType,
              IndexType indexType,
              AttributeValue attributeValue,
              AttributeValue qualifiedAttributeValue) {
        FieldMapping fieldMapping = buildFieldMapping(virtualFieldType, physicalFieldType, indexType);

        AttributeValue actualQualifiedAttributeValue = fieldMapper.apply(fieldMapping, attributeValue);

        assertEquals(qualifiedAttributeValue, actualQualifiedAttributeValue);
    }

    @ParameterizedTest
    @MethodSource("data")
    void testReverse(FieldMapper fieldMapper,
                     ScalarAttributeType virtualFieldType,
                     ScalarAttributeType physicalFieldType,
                     IndexType indexType,
                     AttributeValue attributeValue,
                     AttributeValue qualifiedAttributeValue) {
        FieldMapping fieldMapping = reverseFieldMapping(buildFieldMapping(virtualFieldType, physicalFieldType,
            indexType));

        AttributeValue actualAttributeValue = fieldMapper.reverse(fieldMapping, qualifiedAttributeValue);

        assertEquals(attributeValue, actualAttributeValue);
    }

    static Stream<Object[]> invalidData() {
        return Arrays.stream(new Object[][] {
            { SFM, N, S, new NullPointerException("attributeValue={S: value,} of type=N could not be converted") },
            { SFM, null, S, new NullPointerException("null attribute type") },
            { BFM, N, B, new NullPointerException()},
            { BFM, null, B, new NullPointerException("null attribute type") },
        });
    }

    @ParameterizedTest
    @MethodSource("invalidData")
    void testException(FieldMapper fieldMapper,
                       ScalarAttributeType virtualFieldType,
                       ScalarAttributeType physicalFieldType,
                       Exception expected) {
        FieldMapping fieldMapping = buildFieldMapping(virtualFieldType, physicalFieldType, TABLE);
        try {
            fieldMapper.apply(fieldMapping, new AttributeValue().withS("value"));
            fail("Expected exception not thrown");
        } catch (Exception e) {
            assertEquals(expected.getClass(), e.getClass());
            assertEquals(expected.getMessage(), e.getMessage());
        }
    }

    private FieldMapping buildFieldMapping(ScalarAttributeType sourceFieldType, ScalarAttributeType targetFieldType,
                                           IndexType indexType) {
        return new FieldMapping(
            new Field("sourceField", sourceFieldType),
            new Field("targetField", targetFieldType),
            TABLE_NAME,
            "physicalIndex",
            indexType,
            true);
    }

    private FieldMapping reverseFieldMapping(FieldMapping fieldMapping) {
        return new FieldMapping(
            fieldMapping.getTarget(),
            fieldMapping.getSource(),
            fieldMapping.getVirtualIndexName(),
            fieldMapping.getPhysicalIndexName(),
            fieldMapping.getIndexType(),
            fieldMapping.isContextAware());
    }

}