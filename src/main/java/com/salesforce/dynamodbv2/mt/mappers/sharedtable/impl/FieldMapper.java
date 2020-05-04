package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.function.Predicate;

public interface FieldMapper {

    AttributeValue apply(String context, FieldMapping fieldMapping, AttributeValue unqualifiedAttribute);

    AttributeValue reverse(FieldMapping fieldMapping, AttributeValue qualifiedAttribute);

    Predicate<AttributeValue> createFilter(String context);

}
