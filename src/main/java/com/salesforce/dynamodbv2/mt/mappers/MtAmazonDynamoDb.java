package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.Identity;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import java.util.List;

/**
 * This interface (including all contained interfaces and methods) is
 * experimental. It is subject to breaking changes. Use at your own risk.
 */
public interface MtAmazonDynamoDb extends AmazonDynamoDB {

    class MtRecord extends Record {

        private static final long serialVersionUID = -6099434068333437314L;

        private String context;
        private String tableName;

        public String getContext() {
            return context;
        }

        public MtRecord withContext(String context) {
            this.context = context;
            return this;
        }

        public String getTableName() {
            return tableName;
        }

        public MtRecord withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        @Override
        public MtRecord withAwsRegion(String awsRegion) {
            setAwsRegion(awsRegion);
            return this;
        }

        @Override
        public MtRecord withDynamodb(StreamRecord dynamodb) {
            setDynamodb(dynamodb);
            return this;
        }

        @Override
        public MtRecord withEventID(String eventId) {
            setEventID(eventId);
            return this;
        }

        @Override
        public MtRecord withEventName(OperationType eventName) {
            setEventName(eventName);
            return this;
        }

        @Override
        public MtRecord withEventName(String eventName) {
            setEventName(eventName);
            return this;
        }

        @Override
        public MtRecord withEventSource(String eventSource) {
            setEventSource(eventSource);
            return this;
        }

        @Override
        public MtRecord withEventVersion(String eventVersion) {
            setEventVersion(eventVersion);
            return this;
        }

        @Override
        public MtRecord withUserIdentity(Identity userIdentity) {
            setUserIdentity(userIdentity);
            return this;
        }

        @Override
        public String toString() {
            return "MtRecord{"
                + "context='" + context + '\''
                + ", tableName='" + tableName + '\''
                + ", recordFields=" + super.toString()
                + '}';
        }
    }

    /**
     * Wrap a scan result from a multi tenant scan, to associate tenant information per scan result item.
     **/
    class MtScanResult extends ScanResult {
        private final List<String> tenants;
        private final List<String> virtualTables;

        public MtScanResult(ScanResult scanResult, List<String> tenants, List<String> virtualTables) {
            super();
            this.withLastEvaluatedKey(scanResult.getLastEvaluatedKey());
            this.withConsumedCapacity(scanResult.getConsumedCapacity());
            this.withCount(scanResult.getCount());
            this.withScannedCount(scanResult.getScannedCount());
            this.setSdkHttpMetadata(scanResult.getSdkHttpMetadata());
            this.setSdkResponseMetadata(scanResult.getSdkResponseMetadata());
            this.tenants = tenants;
            this.virtualTables = virtualTables;
        }

        public List<String> getTenants() {
            return tenants;
        }

        public List<String> getVirtualTables() {
            return virtualTables;
        }
    }

}
