/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Maps physical stream records into virtual stream records. Also exposes a filter method to allow pushing tenant table
 * predicate as low as possible when traversing a shared stream.
 */
public interface RecordMapper extends Function<Record, MtRecord> {

    Predicate<Record> createFilter();

}
