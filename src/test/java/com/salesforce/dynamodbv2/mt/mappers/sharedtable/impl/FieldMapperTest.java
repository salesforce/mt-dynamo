/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;
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

    private static final String delimiter = random();

    @Test
    void applyTableIndexNonPolymorphic() {
        MTAmazonDynamoDBContextProvider mtContext = buildMTContext();
        String value = generateValue();
        assertMapper(false,
                     S,
                     TABLE,
                     () -> new AttributeValue().withS(value),
                     mtContext.getContext() + delimiter + value,
                     mtContext);
    }

    @Test
    void applyTableIndexPolymorphic() {
        MTAmazonDynamoDBContextProvider mtContext = buildMTContext();
        String value = generateValue();
        assertMapper(
                     true,
                     S,
                     TABLE,
                     () -> new AttributeValue().withS(value),
                     mtContext.getContext() + delimiter + "virtualtable" + delimiter + value,
                     mtContext);
    }

    @Test
    void applySecondaryIndex() {
        MTAmazonDynamoDBContextProvider mtContext = buildMTContext();
        String value = generateValue();
        assertMapper(true,
                     S,
                     SECONDARYINDEX,
                     () -> new AttributeValue().withS(value),
                     mtContext.getContext() + delimiter + "virtualindex" + delimiter + value,
                     mtContext);
    }

    @Test
    void applyTableIndexNonPolymorphicNumber() {
        MTAmazonDynamoDBContextProvider mtContext = buildMTContext();
        assertMapper(false,
                     N,
                     TABLE,
                     () -> new AttributeValue().withN("123"),
                     mtContext.getContext() + delimiter + "123",
                     mtContext);
    }

    @Test
    void applyTableIndexNonPolymorphicByteArray() {
        MTAmazonDynamoDBContextProvider mtContext = buildMTContext();
        assertMapper(false,
                     B,
                     TABLE,
                     () -> new AttributeValue().withB(Charset.defaultCharset().encode("bytebuffer")),
                     mtContext.getContext() + delimiter + "bytebuffer",
                     mtContext);
    }

    @Test
    void applyValueNotFound() {
        try {
            buildFieldMapper(buildMTContext(), false).apply(buildFieldMapping(N, TABLE), new AttributeValue().withS("value"));
        } catch (NullPointerException e) {
            // expected
            assertEquals("attributeValue={S: value,} of type=N could not be converted", e.getMessage());
        }
    }

    @Test
    void invalidType() {
        try {
            buildFieldMapper(buildMTContext(), false).apply(buildFieldMapping(null, TABLE), new AttributeValue().withS(generateValue()));
        } catch (NullPointerException e) {
            // expected
            assertEquals("null attribute type", e.getMessage());
        }
    }

    private void assertMapper(boolean isPolymorphicTable,
                              ScalarAttributeType fieldType,
                              IndexType indexType,
                              Supplier<AttributeValue> attributeValue,
                              String expectedStringValue,
                              MTAmazonDynamoDBContextProvider mtContext) {
        FieldMapping fieldMapping = buildFieldMapping(fieldType, indexType);
        FieldMapper fieldMapper = buildFieldMapper(mtContext, isPolymorphicTable);
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

    private FieldMapper buildFieldMapper(MTAmazonDynamoDBContextProvider mtContext, boolean isPolyporphicTable) {
        return new FieldMapper(mtContext,
                delimiter,
                "virtualtable",
                isPolyporphicTable);
    }

    private static String random() {
        return randomUUID().toString();
    }

    private MTAmazonDynamoDBContextProvider buildMTContext() {
        return new MTAmazonDynamoDBContextProvider() {
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