/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex.DynamoSecondaryIndexType.GSI;

import com.salesforce.dynamodbv2.mt.mappers.CreateTableRequestBuilder;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescriptionImpl;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.util.Collections;
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

}
