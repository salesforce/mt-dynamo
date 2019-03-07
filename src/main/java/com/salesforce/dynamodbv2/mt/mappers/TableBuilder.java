package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.model.BillingMode;

/**
 * Simple interface to represent a TableBuilder.
 * Note, since the implementation of the classes that implement this interface already existed before this interface was
 * created, other methods can also be added.  It was added when BillingMode support was added.
 */
public interface TableBuilder {
    TableBuilder withBillingMode(BillingMode billingMode);
}
