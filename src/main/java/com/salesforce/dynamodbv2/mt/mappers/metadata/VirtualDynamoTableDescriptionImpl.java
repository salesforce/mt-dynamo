/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.metadata;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.salesforce.dynamodbv2.mt.repo.MtTableDescription;

public class VirtualDynamoTableDescriptionImpl extends DynamoTableDescriptionImpl
    implements VirtualDynamoTableDescription {

    private final boolean isMultitenant;

    public VirtualDynamoTableDescriptionImpl(CreateTableRequest createTableRequest, boolean isMultitenant) {
        super(createTableRequest);
        this.isMultitenant = isMultitenant;
    }

    public VirtualDynamoTableDescriptionImpl(MtTableDescription tableDescription) {
        super(tableDescription);
        this.isMultitenant = tableDescription.isMultitenant();
    }

    @Override
    public boolean isMultitenant() {
        return isMultitenant;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        VirtualDynamoTableDescriptionImpl other = (VirtualDynamoTableDescriptionImpl) obj;
        return super.equals(obj)
            && isMultitenant() == other.isMultitenant();
    }

    @Override
    public int hashCode() {
        int hashCode = super.hashCode();
        hashCode = 31 * hashCode + Boolean.hashCode(isMultitenant());
        return hashCode;
    }
}
