package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import java.util.function.Predicate;

public interface FieldPrefixFunction<V> {

    V apply(FieldValue<V> fieldValue);

    FieldValue<V> reverse(V qualifiedValue);

    Predicate<V> createFilter(String context, String tableName);

}
