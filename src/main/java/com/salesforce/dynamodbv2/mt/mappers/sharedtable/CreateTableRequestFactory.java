/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import java.util.List;
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
     * Takes a virtual table description and returns a CreateTableRequest for the corresponding static physical table
     * if one is found.
     */
    Optional<CreateTableRequest> getStaticPhysicalTable(DynamoTableDescription virtualTableDescription);

    /**
     * Returns a list of CreateTableRequests for the static physical tables created when the factory is initialized.
     */
    List<CreateTableRequest> getStaticPhysicalTables();

    /**
     * Takes a virtual table description and returns a CreateTableRequest for the corresponding physical table to be
     * created dynamically.
     */
    CreateTableRequest getDynamicPhysicalTable(DynamoTableDescription virtualTableDescription);

    /**
     * Returns whether the given table name is of a physical table (static or dynamic) belonging to this factory.
     */
    boolean isPhysicalTable(String tableName);

}