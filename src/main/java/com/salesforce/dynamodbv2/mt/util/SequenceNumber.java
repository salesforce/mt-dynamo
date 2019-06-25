package com.salesforce.dynamodbv2.mt.util;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.google.common.base.MoreObjects;
import java.math.BigInteger;
import java.util.Objects;

final class SequenceNumber implements Comparable<SequenceNumber> {

    static SequenceNumber fromRawValue(String sequenceNumber) {
        try {
            return new SequenceNumber(new BigInteger(sequenceNumber));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static SequenceNumber fromRecord(Record record) {
        return fromRawValue(record.getDynamodb().getSequenceNumber());
    }

    static SequenceNumber fromInt(int value) {
        return new SequenceNumber(BigInteger.valueOf(value));
    }

    private final BigInteger value;

    private SequenceNumber(BigInteger value) {
        this.value = value;
    }

    boolean precedes(SequenceNumber other) {
        return compareTo(other) <= 0;
    }

    SequenceNumber next() {
        return new SequenceNumber(value.add(BigInteger.ONE));
    }

    @Override
    public int compareTo(SequenceNumber o) {
        return value.compareTo(o.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SequenceNumber that = (SequenceNumber) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("value", value)
            .toString();
    }
}
