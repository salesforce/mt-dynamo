package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;

/**
 * Wrapper class that allows application code to handle {@code QueryRequest}s, {@code ScanRequest}s,
 * {@code PutItemRequest}s, {@code UpdateItemRequest}s, and {@code DeleteItemRequest}s consistently.
 *
 * @author msgroi
 */
@VisibleForTesting
public interface RequestWrapper {

    String getIndexName();

    Map<String, String> getExpressionAttributeNames();

    void putExpressionAttributeName(String key, String value);

    Map<String, AttributeValue> getExpressionAttributeValues();

    void putExpressionAttributeValue(String key, AttributeValue value);

    String getPrimaryExpression();

    void setPrimaryExpression(String expression);

    String getFilterExpression();

    void setFilterExpression(String s);

    void setIndexName(String indexName);

    Map<String, Condition> getLegacyExpression();

    void clearLegacyExpression();

    Map<String, AttributeValue> getExclusiveStartKey();

    void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey);

}