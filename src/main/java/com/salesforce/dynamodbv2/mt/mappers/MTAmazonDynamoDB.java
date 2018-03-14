package com.salesforce.dynamodbv2.mt.mappers;

import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.Identity;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * This interface (including all contained interfaces and methods) is
 * experimental. It is subject to breaking changes. Use at your own risk.
 */
public interface MTAmazonDynamoDB extends AmazonDynamoDB {

    public static class MTRecord extends Record {

        private static final long serialVersionUID = -6099434068333437314L;

        private String context;
        private String tableName;

        public String getContext() {
            return context;
        }

        public void setContext(String context) {
            this.context = context;
        }

        public MTRecord withContext(String context) {
            this.context = context;
            return this;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public MTRecord withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        @Override
        public MTRecord withAwsRegion(String awsRegion) {
            super.setAwsRegion(awsRegion);
            return this;
        }

        @Override
        public MTRecord withDynamodb(StreamRecord dynamodb) {
            super.setDynamodb(dynamodb);
            return this;
        }

        @Override
        public MTRecord withEventID(String eventID) {
            super.setEventID(eventID);
            return this;
        }

        @Override
        public MTRecord withEventName(OperationType eventName) {
            super.setEventName(eventName);
            return this;
        }

        @Override
        public MTRecord withEventName(String eventName) {
            super.setEventName(eventName);
            return this;
        }

        @Override
        public MTRecord withEventSource(String eventSource) {
            super.setEventSource(eventSource);
            return this;
        }

        @Override
        public MTRecord withEventVersion(String eventVersion) {
            setEventVersion(eventVersion);
            return this;
        }

        @Override
        public MTRecord withUserIdentity(Identity userIdentity) {
            super.setUserIdentity(userIdentity);
            return this;
        }
    }

    public static class MTStreamDescription {

        private String label;
        private String arn;
        private IRecordProcessorFactory recordProcessorFactory;

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public MTStreamDescription withLabel(String label) {
            setLabel(label);
            return this;
        }

        public String getArn() {
            return arn;
        }

        public void setArn(String arn) {
            this.arn = arn;
        }

        public MTStreamDescription withArn(String arn) {
            setArn(arn);
            return this;
        }

        public IRecordProcessorFactory getRecordProcessorFactory() {
            return recordProcessorFactory;
        }

        public void setRecordProcessorFactory(IRecordProcessorFactory recordProcessorFactory) {
            this.recordProcessorFactory = recordProcessorFactory;
        }

        public MTStreamDescription withRecordProcessorFactory(IRecordProcessorFactory recordProcessorFactory) {
            this.recordProcessorFactory = recordProcessorFactory;
            return this;
        }

    }

    List<MTStreamDescription> listStreams(IRecordProcessorFactory factory);

}
