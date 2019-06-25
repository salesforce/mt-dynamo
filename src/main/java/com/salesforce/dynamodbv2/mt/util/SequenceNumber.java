package com.salesforce.dynamodbv2.mt.util;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.google.common.base.MoreObjects;
import java.math.BigInteger;
import java.util.Objects;

/**
 * Representation of a sequence number in a DynamoDB Stream. Similar to KCL's
 * <a href="https://github.com/awslabs/amazon-kinesis-client/blob/6c64055d9b81b51480ab844be1769a3637c2c29c/amazon-kinesis-client/src/main/java/software/amazon/kinesis/retrieval/kpl/ExtendedSequenceNumber.java">ExtendedSequenceNumber</a>
 * but without <code>subSequenceNumber</code>, since DynamoDB does not have those, and parses sequence numbers eagerly,
 * since
 */
final class SequenceNumber implements Comparable<SequenceNumber> {

    /**
     * Parses a sequence number from a DynamoDB Stream String representation.
     *
     * @param sequenceNumber Raw value.
     * @return Parsed SequenceNumber.
     */
    static SequenceNumber fromRawValue(String sequenceNumber) {
        try {
            return new SequenceNumber(new BigInteger(sequenceNumber));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Parses a sequence number contained in the given DynamoDB Stream Record for convenience.
     *
     * @param record DynamoDB record containing a sequence number.
     * @return Parsed SequenceNumber.
     */
    static SequenceNumber fromRecord(Record record) {
        return fromRawValue(record.getDynamodb().getSequenceNumber());
    }

    private final BigInteger value;

    private SequenceNumber(BigInteger value) {
        this.value = value;
    }

    /**
     * Returns the next sequence number in the stream.
     *
     * @return Next sequence number.
     */
    SequenceNumber next() {
        return new SequenceNumber(value.add(BigInteger.ONE));
    }

    /**
     * Comparse this sequence number to the given one in terms of their relative position in the stream.
     *
     * @param o Other sequence number.
     * @return A negative integer, zero, or a positive integer as this sequence number precedes, equals, or succeeds the
     *     specified sequence number in the stream.
     */
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