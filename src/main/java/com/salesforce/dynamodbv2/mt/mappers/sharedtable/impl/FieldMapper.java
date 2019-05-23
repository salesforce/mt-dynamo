package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public interface FieldMapper {

    AttributeValue apply(FieldMapping fieldMapping, AttributeValue unqualifiedAttribute);

    AttributeValue reverse(FieldMapping fieldMapping, AttributeValue qualifiedAttribute);

}
