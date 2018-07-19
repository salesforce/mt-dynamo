/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.Field;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.util.function.Supplier;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.SECONDARYINDEX;
import static com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.FieldMapping.IndexType.TABLE;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;

/*
 * @author msgroi
 */
class FieldMapperTest {

    //    private static final String delimiter = random(); // TODO flip this back on when escaping is implemented
    private static final String delimiter = ".";

    @Test
    void applyTableIndex() {
        MtAmazonDynamoDbContextProvider mtContext = buildMtContext();
        String value = generateValue();
        assertMapper(S,
            TABLE,
            () -> new AttributeValue().withS(value),
            mtContext.getContext() + delimiter + "virtualtable" + delimiter + value,
            mtContext);
    }

    @Test
    void applySecondaryIndex() {
        MtAmazonDynamoDbContextProvider mtContext = buildMtContext();
        String value = generateValue();
        assertMapper(S,
            SECONDARYINDEX,
            () -> new AttributeValue().withS(value),
            mtContext.getContext() + delimiter + "virtualindex" + delimiter + value,
            mtContext);
    }

    @Test
    void applyTableIndexNumber() {
        MtAmazonDynamoDbContextProvider mtContext = buildMtContext();
        assertMapper(N,
            TABLE,
            () -> new AttributeValue().withN("123"),
            mtContext.getContext() + delimiter + "virtualtable" + delimiter + "123",
            mtContext);
    }

    @Test
    void applyTableIndexByteArray() {
        MtAmazonDynamoDbContextProvider mtContext = buildMtContext();
        assertMapper(B,
            TABLE,
            () -> new AttributeValue().withB(Charset.defaultCharset().encode("bytebuffer")),
            mtContext.getContext() + delimiter + "virtualtable" + delimiter + "bytebuffer",
            mtContext);
    }

    @Test
    void applyValueNotFound() {
        try {
            buildFieldMapper(buildMtContext()).apply(buildFieldMapping(N, TABLE), new AttributeValue().withS("value"));
        } catch (NullPointerException e) {
            // expected
            assertEquals("attributeValue={S: value,} of type=N could not be converted", e.getMessage());
        }
    }

    @Test
    void invalidType() {
        try {
            buildFieldMapper(buildMtContext()).apply(buildFieldMapping(null, TABLE), new AttributeValue().withS(generateValue()));
        } catch (NullPointerException e) {
            // expected
            assertEquals("null attribute type", e.getMessage());
        }
    }

    private void assertMapper(ScalarAttributeType fieldType,
                              IndexType indexType,
                              Supplier<AttributeValue> attributeValue,
                              String expectedStringValue,
                              MtAmazonDynamoDbContextProvider mtContext) {
        FieldMapping fieldMapping = buildFieldMapping(fieldType, indexType);
        FieldMapper fieldMapper = buildFieldMapper(mtContext);
        AttributeValue qualifiedAttributeValue = fieldMapper.apply(fieldMapping, attributeValue.get());
        assertEquals(expectedStringValue, qualifiedAttributeValue.getS());
        AttributeValue actualAttributeValue = fieldMapper.reverse(reverseFieldMapping(fieldMapping), qualifiedAttributeValue);
        assertEquals(attributeValue.get(), actualAttributeValue);
    }

    private FieldMapping buildFieldMapping(ScalarAttributeType sourceFieldType, IndexType indexType) {
        return new FieldMapping(
            new Field("sourcefield", sourceFieldType),
            new Field("targetfield", S),
            "virtualindex",
            "physicalindex",
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

    private FieldMapper buildFieldMapper(MtAmazonDynamoDbContextProvider mtContext) {
        return new FieldMapper(mtContext,
            "virtualtable",
            new FieldPrefixFunction(delimiter));
    }

    private static String random() {
        return randomUUID().toString();
    }

    private MtAmazonDynamoDbContextProvider buildMtContext() {
        return new MtAmazonDynamoDbContextProvider() {
            String context = random();

            @Override
            public String getContext() {
                return context;
            }
        };
    }

    private String generateValue() {
        return random();
    }

}