package com.salesforce.dynamodbv2.mt.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl.BASE_CONTEXT;

import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/**
 * Stream ARN that includes a virtual table name in addition to the physical table name and stream label. See <a
 * href="https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arns-syntax">Amazon Resource Names
 * (ARNs) and AWS Service Namespaces</a> for details on the ARN format.
 */
public class StreamArn {

    static class MtStreamArn extends StreamArn {

        private static final String VIRTUAL_FORMAT =
            "%s" + RESOURCE_SEPARATOR + CONTEXT_SEGMENT + "%s" + RESOURCE_SEPARATOR + TENANT_TABLE_SEGMENT + "%s";

        private final String context; // (not URL encoded)
        private final String tenantTableName; // (not URL encoded)

        MtStreamArn(String prefix, String tableName, String streamLabel, String context, String tenantTableName) {
            super(prefix, tableName, streamLabel);
            this.context = context;
            this.tenantTableName = tenantTableName;
        }

        @Override
        public String getContext() {
            return this.context;
        }

        @Override
        public Optional<String> getTenantTableName() {
            return Optional.of(tenantTableName);
        }

        @Override
        public boolean matches(MtRecord record) {
            return this.getContext().equals(record.getContext())
                && this.getTenantTableName().equals(Optional.of(record.getTableName()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            MtStreamArn that = (MtStreamArn) o;
            return Objects.equals(context, that.context) && Objects.equals(tenantTableName, that.tenantTableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), context, tenantTableName);
        }

        // URL encodes {@code context} and {@code tenantTableName}
        // TODO: don't re-encode on multiple calls to {@code toString}? (i.e., do it once and save it)?
        @Override
        public String toString() {
            return String.format(VIRTUAL_FORMAT, super.toString(), wrappedEncoder(context),
                wrappedEncoder(tenantTableName));
        }

    }

    // unchecks potential UnsupportedEncodingException (as IllegalArgumentException)
    private static String wrappedDecoder(String s) {
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("Cannot decode " + s, uee);
        }
    }

    // unchecks potential UnsupportedEncodingException (as IllegalArgumentException)
    private static String wrappedEncoder(String s) {
        try {
            return URLEncoder.encode(s, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("Cannot encode " + s, uee);
        }
    }

    private static final char QUALIFIER_SEPARATOR = ':';
    private static final char RESOURCE_SEPARATOR = '/';
    private static final String ARN_PREFIX = "arn" + QUALIFIER_SEPARATOR;
    private static final String TABLE_SEGMENT = "table" + RESOURCE_SEPARATOR;
    private static final String STREAM_SEGMENT = "stream" + RESOURCE_SEPARATOR;
    private static final String CONTEXT_SEGMENT = "context" + RESOURCE_SEPARATOR;
    private static final String TENANT_TABLE_SEGMENT = "tenantTable" + RESOURCE_SEPARATOR;
    private static final String FORMAT =
        ARN_PREFIX + "%s" + TABLE_SEGMENT + "%s" + RESOURCE_SEPARATOR + STREAM_SEGMENT + "%s";

    /**
     * Parses ARN from string value and assigns the given context and tenant table.
     *
     * @param arn Arn to parse.
     * @param unencodedContext Tenant context (not URL encoded).
     * @param unencodedMtTableName Tenant table (not URL encoded).
     * @return Parsed arn.
     */
    public static StreamArn fromString(String arn, String unencodedContext, String unencodedMtTableName) {
        final StreamArn streamArn = fromString(arn);
        return new MtStreamArn(streamArn.qualifier, streamArn.tableName, streamArn.streamLabel, unencodedContext,
            unencodedMtTableName);
    }

    /**
     * Parses arn from string value. "context" and "tenantTable" segments must be URL encoded.
     *
     * @param arn String value.
     * @return Parsed arn.
     */
    public static StreamArn fromString(String arn) {
        // arn prefix
        checkArgument(arn.startsWith(ARN_PREFIX), "ARN missing '" + ARN_PREFIX + "' qualifier");
        int start = ARN_PREFIX.length();
        int end = start;

        // qualifier (partition, service, region, and accountId)
        for (int i = 0; i < 4; i++) {
            end = arn.indexOf(QUALIFIER_SEPARATOR, end) + 1;
            checkArgument(end > 0);
        }
        String qualifier = arn.substring(start, end);

        // table name
        start = end;
        checkArgument(arn.regionMatches(start, TABLE_SEGMENT, 0, TABLE_SEGMENT.length()));
        start += TABLE_SEGMENT.length();
        end = arn.indexOf(RESOURCE_SEPARATOR, start);
        checkArgument(end != -1);
        String tableName = arn.substring(start, end);

        // stream label
        start = end + 1;
        checkArgument(arn.regionMatches(start, STREAM_SEGMENT, 0, STREAM_SEGMENT.length()));
        start += STREAM_SEGMENT.length();
        end = arn.indexOf(RESOURCE_SEPARATOR, start);
        String streamLabel = end == -1 ? arn.substring(start) : arn.substring(start, end);

        // no tenant part (standard DynamoDB ARN)
        if (end == -1) {
            return new StreamArn(qualifier, tableName, streamLabel);
        }

        // tenant context
        start = end + 1;
        checkArgument(arn.regionMatches(start, CONTEXT_SEGMENT, 0, CONTEXT_SEGMENT.length()));
        start += CONTEXT_SEGMENT.length();
        end = arn.indexOf(RESOURCE_SEPARATOR, start);
        checkArgument(end != -1);
        final String decodedContext = wrappedDecoder(arn.substring(start, end));

        // tenant table
        start = end + 1;
        checkArgument(arn.regionMatches(start, TENANT_TABLE_SEGMENT, 0, TENANT_TABLE_SEGMENT.length()));
        start += TENANT_TABLE_SEGMENT.length();
        end = arn.indexOf(RESOURCE_SEPARATOR, start);
        checkArgument(end == -1);
        final String decodedTenantTableName = wrappedDecoder(arn.substring(start));

        return new MtStreamArn(qualifier, tableName, streamLabel, decodedContext, decodedTenantTableName);
    }

    private final String qualifier;
    private final String tableName;
    private final String streamLabel;

    StreamArn(String qualifier, String tableName, String streamLabel) {
        this.qualifier = qualifier;
        this.tableName = tableName;
        this.streamLabel = streamLabel;
    }

    /**
     * Returns the table name contained in this arn.
     *
     * @return Table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the context contained in this arn.
     *
     * @return Context in this arn.
     */
    public String getContext() {
        return BASE_CONTEXT;
    }

    /**
     * Returns the tenant table name in this arn.
     *
     * @return Tenant table name in this arn. May be empty.
     */
    public Optional<String> getTenantTableName() {
        return Optional.empty();
    }

    /**
     * Checks whether this arn matches the given record given the context and tenantTableName configured in it.
     *
     * @param record Record to test
     * @return true if arn context and table name matches record, false otherwise.
     */
    public boolean matches(MtRecord record) {
        return true;
    }

    /**
     * Returns the DynamoDB-compatible representation of this arn. Omits context and mtTable if present.
     *
     * @return DynamoDB-compatible representation.
     */
    public String toDynamoDbArn() {
        return String.format(FORMAT, qualifier, tableName, streamLabel);
    }

    @Override
    public String toString() {
        return toDynamoDbArn();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamArn that = (StreamArn) o;
        return Objects.equals(qualifier, that.qualifier)
            && Objects.equals(tableName, that.tableName)
            && Objects.equals(streamLabel, that.streamLabel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifier, tableName, streamLabel);
    }
}
