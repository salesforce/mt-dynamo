/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.repo;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.TenantTable;
import java.util.Map;

/**
 * Interface for managing table metadata of multi-tenant virtual tables within mt-dynamo.
 *
 * @author msgroi
 */
public interface MtTableDescriptionRepo {

    /**
     * Create a multi-tenant (virtual) table with specs defined in @param createTableRequest under the given
     * multi-tenant contexts' namespace.
     *
     * @return the table description of said table
     */
    TableDescription createTable(CreateTableRequest createTableRequest);

    /**
     * Get the table description of the given virtual table under the current multi-tenant context.
     */
    TableDescription getTableDescription(String tableName);

    /**
     * Delete the designated virtual table metadata.
     */
    TableDescription deleteTable(String tableName);

    /**
     * @return all multi tenant tables maintained by this instance of mt-dynamo.
     */
    Map<TenantTable, CreateTableRequest> getAllMtTables();

}