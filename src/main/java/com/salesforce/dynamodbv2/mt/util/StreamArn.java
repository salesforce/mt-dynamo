package com.salesforce.dynamodbv2.mt.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Stream ARN that includes a virtual table name in addition to the physical table name and stream label. See <a
 * href="https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arns-syntax">Amazon Resource Names
 * (ARNs) and AWS Service Namespaces</a> for details on the ARN format.
 */
public class StreamArn {

    static class MtStreamArn extends StreamArn {

        private static final String VIRTUAL_FORMAT = "%s/context/%s/mttable/%s";

        private final String context;
        private final String mtTableName;

        MtStreamArn(String partition, String service, String region, String accountId, String tableName,
            String streamLabel, String context, String mtTableName) {
            super(partition, service, region, accountId, tableName, streamLabel);
            this.context = context;
            this.mtTableName = mtTableName;
        }

        @Override
        public Optional<String> getContextOpt() {
            return Optional.of(context);
        }

        @Override
        public boolean matches(MtRecord record) {
            return context.equals(record.getContext()) && mtTableName.equals(record.getTableName());
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
            return Objects.equals(context, that.context)
                && Objects.equals(mtTableName, that.mtTableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), context, mtTableName);
        }

        @Override
        public String toString() {
            return String.format(VIRTUAL_FORMAT, super.toString(), context, mtTableName);
        }
    }

    private static final String FORMAT = "arn:%s:%s:%s:%s:table/%s/stream/%s";

    private static final Pattern ARN_PATTERN = Pattern.compile("arn:(?<partition>[^:]+):(?<service>[^:]+):"
        + "(?<region>[^:]+):(?<accountId>[^:]+):table/(?<tableName>[^/]+)/stream/(?<streamLabel>[^/]+)"
        + "(?:/context/(?<context>[^/]+)/mttable/(?<mtTableName>[^/]+))?");

    /**
     * Parses arn from string value and assigns the given context and tenant table.
     *
     * @param arn Arn to parse.
     * @param context Tenant context.
     * @param mtTableName Tenant table.
     * @return Parsed arn.
     */
    public static StreamArn fromString(String arn, String context, String mtTableName) {
        StreamArn streamArn = fromString(arn);
        return new MtStreamArn(streamArn.partition, streamArn.service, streamArn.region, streamArn.accountId,
            streamArn.tableName, streamArn.streamLabel, context, mtTableName);
    }

    /**
     * Parses arn from string value.
     *
     * @param arn String value.
     * @return Parsed arn.
     */
    public static StreamArn fromString(String arn) {
        System.out.println(arn);
        Matcher m = ARN_PATTERN.matcher(arn);
        checkArgument(m.matches());
        String partition = m.group("partition");
        String service = m.group("service");
        String region = m.group("region");
        String accountId = m.group("accountId");
        String tableName = m.group("tableName");
        String streamLabel = m.group("streamLabel");
        Optional<String> context = Optional.ofNullable(m.group("context"));
        Optional<String> virtualTableName = Optional.ofNullable(m.group("mtTableName"));

        if (context.isPresent() && virtualTableName.isPresent()) {
            return new MtStreamArn(partition, service, region, accountId, tableName, streamLabel, context.get(),
                virtualTableName.get());
        } else if (!context.isPresent() && !virtualTableName.isPresent()) {
            return new StreamArn(partition, service, region, accountId, tableName, streamLabel);
        } else {
            throw new IllegalArgumentException("Invalid stream arn " + arn);
        }
    }

    private final String partition;
    private final String service;
    private final String region;
    private final String accountId;
    private final String tableName;
    private final String streamLabel;

    StreamArn(String partition, String service, String region, String accountId,
        String tableName, String streamLabel) {
        this.partition = partition;
        this.service = service;
        this.region = region;
        this.accountId = accountId;
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
    public Optional<String> getContextOpt() {
        return Optional.empty();
    }

    /**
     * Checks whether this arn matches the given record given the context and mtTableName configured in it.
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
        return String.format(FORMAT, partition, service, region, accountId, tableName, streamLabel);
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
        return Objects.equals(partition, that.partition)
            && Objects.equals(service, that.service)
            && Objects.equals(region, that.region)
            && Objects.equals(accountId, that.accountId)
            && Objects.equals(tableName, that.tableName)
            && Objects.equals(streamLabel, that.streamLabel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, service, region, accountId, tableName, streamLabel);
    }
}
