package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

public interface ConditionMapper {

    static final String NAME_PLACEHOLDER = "#___name___";

    void apply(RequestWrapper request);
}
