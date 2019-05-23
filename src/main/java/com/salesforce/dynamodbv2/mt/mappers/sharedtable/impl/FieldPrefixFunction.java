package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

public interface FieldPrefixFunction<V> {

    V apply(FieldValue<V> fieldValue);

    FieldValue<V> reverse(V qualifiedValue);

}
