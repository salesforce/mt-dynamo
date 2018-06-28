package com.salesforce.dynamodbv2.mt.mappers.index;

import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;

import java.util.List;

/*
 * @author msgroi
 */
public interface PrimaryKeyMapper {

    HasPrimaryKey mapPrimaryKey(PrimaryKey primaryKeyToFind, List<HasPrimaryKey> primaryKeys) throws MappingException;

}
