package com.salesforce.dynamodbv2.mt.mappers;

import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;

public interface MTAmazonDynamoDB extends AmazonDynamoDB {

	@FunctionalInterface
	interface MTIRecordProcessorFactory {

		@FunctionalInterface
		interface Adapter {
			IRecordProcessorFactory adapt(MTIRecordProcessorFactory factory);
		}

		MTIRecordProcessor createProcessor();
	}

	public interface MTIRecordProcessor {

		void initialize(String shardId);

		void processRecords(String tenant, String tableName, List<Record> records);

		void shutdown(ShutdownReason reason);

	}

	public static class MTStreamDescription {

		private String label;
		private String arn;
		private MTIRecordProcessorFactory.Adapter factoryAdapter;

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

		public MTIRecordProcessorFactory.Adapter getFactoryAdapter() {
			return factoryAdapter;
		}

		public void setFactoryAdapter(MTIRecordProcessorFactory.Adapter factoryAdapter) {
			this.factoryAdapter = factoryAdapter;
		}

		public MTStreamDescription withFactoryAdapter(MTIRecordProcessorFactory.Adapter factoryAdapter) {
			setFactoryAdapter(factoryAdapter);
			return this;
		}
	}

	List<MTStreamDescription> listStreams();

	AmazonDynamoDB getAmazonDynamoDB();

}
