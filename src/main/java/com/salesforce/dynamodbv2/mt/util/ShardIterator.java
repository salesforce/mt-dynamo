package com.salesforce.dynamodbv2.mt.util;

import java.util.Objects;

/**
 * The shard iterator format we assume is <code>{arn}|{rest}</code>. Local DynamoDB prepends <i>000|</i>, so we support
 * that as well.
 */
public class ShardIterator {

    public static final char ITERATOR_SEPARATOR = '|';
    private static final String LOCAL_DYNAMODB_PREFIX = "000" + ITERATOR_SEPARATOR;

    /**
     * Parses shard iterator from string representation.
     *
     * @param value String value of iterator.
     * @return Shard iterator.
     */
    public static ShardIterator fromString(String value) {
        final boolean local = value.startsWith(LOCAL_DYNAMODB_PREFIX);
        if (local) {
            value = value.substring(LOCAL_DYNAMODB_PREFIX.length());
        }
        final int idx = value.indexOf(ITERATOR_SEPARATOR);
        final String arn = value.substring(0, idx);
        final String rest = value.substring(idx + 1);
        return new ShardIterator(local, arn, rest);
    }

    private final boolean local;
    private final String arn;
    private final String rest;

    private ShardIterator(boolean local, String arn, String rest) {
        this.local = local;
        this.arn = arn;
        this.rest = rest;
    }

    public String getArn() {
        return arn;
    }

    public ShardIterator withArn(String arn) {
        return new ShardIterator(local, arn, rest);
    }

    public String getRest() {
        return rest;
    }

    public ShardIterator withRest(String rest) {
        return new ShardIterator(local, arn, rest);
    }

    @Override
    public String toString() {
        String value = arn + ITERATOR_SEPARATOR + rest;
        if (local) {
            value = LOCAL_DYNAMODB_PREFIX + value;
        }
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
        final ShardIterator that = (ShardIterator) o;
        return local == that.local && Objects.equals(arn, that.arn) && Objects.equals(rest, that.rest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(local, arn, rest);
    }
}
