/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TableMappingTestUtil {

    static DynamoTableDescription buildTable(String tableName, PrimaryKey primaryKey) {
        return buildTable(tableName, primaryKey, Collections.emptyMap());
    }

    static DynamoTableDescription buildTable(String tableName, PrimaryKey primaryKey, Map<String, PrimaryKey> gsis) {
        CreateTableRequestBuilder createTableRequestBuilder = CreateTableRequestBuilder.builder()
            .withTableName(tableName);
        if (primaryKey.getRangeKey().isPresent()) {
            createTableRequestBuilder.withTableKeySchema(primaryKey.getHashKey(), primaryKey.getHashKeyType(),
                primaryKey.getRangeKey().get(), primaryKey.getRangeKeyType().get());
        } else {
            createTableRequestBuilder.withTableKeySchema(primaryKey.getHashKey(), primaryKey.getHashKeyType());
        }
        gsis.forEach((name, pk) -> createTableRequestBuilder.addSi(name, GSI, pk, 1L));
        return new DynamoTableDescriptionImpl(createTableRequestBuilder.build());
    }

    /**
     * Verifies the resulting UpdateItemRequest after {@link ConditionMapper#applyForUpdate(UpdateItemRequest)}.
     * It's a bit ugly because we don't know what the expected placeholder for each field or value in the update
     * expression is unless we know exactly the order in which things are iterated over. So we instead convert the
     * update expression back into a field-value map, so placeholders are irrelevant, and compare it with what we
     * expect.
     */
    static void verifyApplyToUpdate(UpdateItemRequest request,
                                    Map<String, AttributeValue> expectedUpdateItem,
                                    Map<String, String> conditionExpressionFieldPlaceholders,
                                    Map<String, AttributeValue> conditionExpressionValuePlaceholders) {
        assertTrue(request.getUpdateExpression().startsWith("SET "));
        String[] setActions = request.getUpdateExpression().substring("SET ".length()).split(", ");
        assertEquals(expectedUpdateItem.size(), setActions.length);

        Map<String, AttributeValue> actualUpdateItem = new HashMap<>();
        for (String setAction : setActions) {
            String[] fieldAndValue = setAction.split(" = ");
            assertEquals(2, fieldAndValue.length);

            String fieldLiteral = request.getExpressionAttributeNames().get(fieldAndValue[0]);
            AttributeValue valueLiteral = request.getExpressionAttributeValues().get(fieldAndValue[1]);
            actualUpdateItem.put(fieldLiteral, valueLiteral);
        }
        assertEquals(expectedUpdateItem, actualUpdateItem);

        if (conditionExpressionFieldPlaceholders != null) {
            conditionExpressionFieldPlaceholders.forEach((placeholder, field)
                -> assertEquals(field, request.getExpressionAttributeNames().get(placeholder)));
        }
        if (conditionExpressionValuePlaceholders != null) {
            conditionExpressionValuePlaceholders.forEach((placeholder, value)
                -> assertEquals(value, request.getExpressionAttributeValues().get(placeholder)));
        }
    }
}
