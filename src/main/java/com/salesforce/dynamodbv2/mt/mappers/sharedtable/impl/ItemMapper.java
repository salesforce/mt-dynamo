package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.Map;

public interface ItemMapper {

    Map<String, AttributeValue> apply(Map<String, AttributeValue> unqualifiedItem);

    Map<String, AttributeValue> reverse(Map<String, AttributeValue> qualifiedItem);

}
