/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;

/**
 * Interface indicating that a class has primary key, represented by a PrimaryKey.  DynamoDB tables have primary keys
 * as do secondary indexes, thus this class is implemented by DynamoTableDescriptionImpl, DynamoSecondaryIndex, as
 * well as CreateTableRequestWrapper.
 *
 * @author msgroi
 */
public interface HasPrimaryKey {

    PrimaryKey getPrimaryKey();

}