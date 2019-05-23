package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

class FieldValue<V> {

    private final String context;
    private final String tableName;
    private final V value;

    FieldValue(String context, String tableName, V value) {
        this.context = context;
        this.tableName = tableName;
        this.value = value;
    }

    String getContext() {
        return context;
    }

    String getTableName() {
        return tableName;
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

        FieldValue that = (FieldValue) o;

        return context.equals(that.context)
                && tableName.equals(that.tableName)
                && value.equals(that.value);
    }

    @Override
    public String toString() {
        return "FieldValue{"
                + "context='" + context + '\''
                + ", tableName='" + tableName + '\''
                + ", value='" + value + '\''
                + '}';
    }

}
