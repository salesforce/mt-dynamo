package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.google.common.base.Objects;

class FieldValue<V> extends MtContextAndTable {

    private final V value;

    FieldValue(String context, String tableName, V value) {
        super(context, tableName);
        this.value = value;
    }

    V getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FieldValue<?> that = (FieldValue<?>) o;
        return super.equals(that)
                && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getContext(), getTableName(), this.value);
    }

    @Override
    public String toString() {
        return "FieldValue{"
                + "context='" + getContext() + '\''
                + ", tableName='" + getTableName() + '\''
                + ", value='" + value + '\''
                + '}';
    }

}
