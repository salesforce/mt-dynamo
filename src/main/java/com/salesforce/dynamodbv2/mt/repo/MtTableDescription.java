/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.repo;

import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * Virtual table metadata record stored in the table description repo.
 */
public class MtTableDescription extends TableDescription {

    private boolean isMultitenant;

    public MtTableDescription() {
        super();
    }

    public boolean isMultitenant() {
        return isMultitenant;
    }

    public MtTableDescription withMultitenant(boolean isMultitenant) {
        this.isMultitenant = isMultitenant;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MtTableDescription other = (MtTableDescription) obj;
        return super.equals(obj)
            && isMultitenant() == other.isMultitenant();
    }

    @Override
    public int hashCode() {
        int hashCode = super.hashCode();
        hashCode = 31 * hashCode + Boolean.hashCode(isMultitenant());
        return hashCode;
    }

    @Override
    public String toString() {
        return super.toString() + "{IsMultitenant: " + isMultitenant() + "}";
    }
}
