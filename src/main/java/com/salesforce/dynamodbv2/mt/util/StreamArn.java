package com.salesforce.dynamodbv2.mt.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Preconditions;
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

        private final String context; // must have no '/'s
        private final String tenantTableName; // must have no '/'s

        MtStreamArn(String prefix,
            String tableName,
            String streamLabel,
            String slashFreeContext,
            String slashFreeTenantTableName) {
            super(prefix, tableName, streamLabel);
            Preconditions.checkArgument(!slashFreeContext.contains("/"),
                "slashFreeContext parameter must not contain '/'s: " + slashFreeContext);
            Preconditions.checkArgument(!slashFreeTenantTableName.contains("/"),
                "slashFreeTenantTableName parameter must not contain '/'s: " + slashFreeTenantTableName);
            this.context = slashFreeContext;
            this.tenantTableName = slashFreeTenantTableName;
        }

        /**
         * Get the URL-decoded version of the context.
         */
        @Override
        public Optional<String> getContext() {
            return Optional.of(wrappedDecoder(context));
        }

        /**
         * Get the URL-decoded version of the tenantTableName.
         */
        @Override
        public Optional<String> getTenantTableName() {
            return Optional.of(wrappedDecoder(tenantTableName));
        }

        @Override
        public boolean matches(MtRecord record) {
            return this.getContext().equals(Optional.of(record.getContext()))
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

        // NB: {@code context} and {@code tenantTableName} are URL encoded.
        @Override
        public String toString() {
            return String.format(VIRTUAL_FORMAT, super.toString(), context, tenantTableName);
        }

    }

    // TODO: don't decode every time (i.e., do it once and save it)?
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
     * Parses ARN from string value and assigns the given context and tenant table, URL encoding both (since the
     * {@code MtStreamArn} constructor requires no '/'s in its last two arguments.
     *
     * @param arn Arn to parse.
     * @param unencodedContext Tenant context.
     * @param unencodedMtTableName Tenant table.
     * @return Parsed arn.
     */
    public static StreamArn fromString(String arn, String unencodedContext, String unencodedMtTableName) {
        final StreamArn streamArn = fromString(arn);
        return new MtStreamArn(streamArn.qualifier, streamArn.tableName, streamArn.streamLabel,
            wrappedEncoder(unencodedContext), wrappedEncoder(unencodedMtTableName));
    }

    /**
     * Parses arn from string value. "context" and "tenantTable" segments must have no '/'s.
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
        final String slashFreeContext = arn.substring(start, end);

        // tenant table
        start = end + 1;
        checkArgument(arn.regionMatches(start, TENANT_TABLE_SEGMENT, 0, TENANT_TABLE_SEGMENT.length()));
        start += TENANT_TABLE_SEGMENT.length();
        end = arn.indexOf(RESOURCE_SEPARATOR, start);
        checkArgument(end == -1);
        final String slashFreeTenantTableName = arn.substring(start);

        return new MtStreamArn(qualifier, tableName, streamLabel, slashFreeContext, slashFreeTenantTableName);
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
     * @return Context in this arn. May be empty.
     */
    public Optional<String> getContext() {
        return Optional.empty();
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
