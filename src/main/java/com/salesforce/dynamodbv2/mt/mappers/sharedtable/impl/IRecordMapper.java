package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Maps physical stream records into virtual stream records. Also exposes a filter method to allow pushing tenant table
 * predicate as low as possible when traversing a shared stream.
 */
public interface IRecordMapper extends Function<Record, MtRecord> {

    Predicate<Record> createFilter();

}
