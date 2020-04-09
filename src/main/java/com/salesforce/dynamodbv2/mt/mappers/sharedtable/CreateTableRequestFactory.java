/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import java.util.Optional;

/**
 * Allows a client to provide a custom mapping of virtual {@code CreateTableRequest}s to physical ones.
 *
 * <p>See {@code TableMappingFactory} for details.
 *
 * @author msgroi
 */
public interface CreateTableRequestFactory {

    /**
     * Takes a virtual table description and returns a CreateTableRequest corresponding physical table if one is found.
     */
    Optional<CreateTableRequest> getCreateTableRequest(DynamoTableDescription virtualTableDescription);

    /**
     * Returns whether the given physical table name belongs to a table that can be created by this factory.
     */
    boolean isPhysicalTable(String physicalTableName);

}