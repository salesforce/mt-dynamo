/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.repo;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
public interface MtTableDescriptionRepo {

    TableDescription createTable(CreateTableRequest createTableRequest);

    TableDescription getTableDescription(String tableName);

    TableDescription deleteTable(String tableName);

    void invalidateCaches();

}