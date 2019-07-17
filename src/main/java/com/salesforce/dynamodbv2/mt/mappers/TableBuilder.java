package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.model.BillingMode;

/**
 * Simple interface to represent a TableBuilder.
 * Note that since the implementation of the classes that implement this interface already existed before this interface
 * was created, other methods can also be added.  It was added when BillingMode support was added.
 */
public interface TableBuilder {
    TableBuilder withBillingMode(BillingMode billingMode);

    /**
     * When performing a scan across a table without a tenant context provided, encode the tenant key of each row
     * encoded with this column name on the returned result set.
     */
    TableBuilder withScanTenantKey(String scanTenantKey);

    /**
     * When performing a scan across a table without a tenant context provided, encode the customer-provided table name
     * of each row encoded with this column name on the returned result set.
     */
    TableBuilder withScanVirtualTableKey(String scanVirtualTableKey);
}
