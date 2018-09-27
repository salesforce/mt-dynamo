/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.metadata;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.index.HasPrimaryKey;

import java.util.List;
import java.util.Optional;

/**
 * Interface that allows {@code DynamoTableDescriptionImpl}s and {@code CreateTableRequest}s to be used interchangeably.
 *
 * @author msgroi
 */
public interface DynamoTableDescription extends HasPrimaryKey {

    String getTableName();

    PrimaryKey getPrimaryKey();

    List<DynamoSecondaryIndex> getSis();

    List<DynamoSecondaryIndex> getGsis();

    Optional<DynamoSecondaryIndex> getGsi(String indexName);

    List<DynamoSecondaryIndex> getLsis();

    Optional<DynamoSecondaryIndex> getLsi(String indexName);

    DynamoSecondaryIndex findSi(String indexName);

    StreamSpecification getStreamSpecification();

    String getLastStreamArn();

    CreateTableRequest getCreateTableRequest();

}