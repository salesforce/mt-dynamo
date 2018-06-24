/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.DeleteAlarmsRequest;
import com.amazonaws.services.cloudwatch.model.DeleteAlarmsResult;
import com.amazonaws.services.cloudwatch.model.DeleteDashboardsRequest;
import com.amazonaws.services.cloudwatch.model.DeleteDashboardsResult;
import com.amazonaws.services.cloudwatch.model.DescribeAlarmHistoryRequest;
import com.amazonaws.services.cloudwatch.model.DescribeAlarmHistoryResult;
import com.amazonaws.services.cloudwatch.model.DescribeAlarmsForMetricRequest;
import com.amazonaws.services.cloudwatch.model.DescribeAlarmsForMetricResult;
import com.amazonaws.services.cloudwatch.model.DescribeAlarmsRequest;
import com.amazonaws.services.cloudwatch.model.DescribeAlarmsResult;
import com.amazonaws.services.cloudwatch.model.DisableAlarmActionsRequest;
import com.amazonaws.services.cloudwatch.model.DisableAlarmActionsResult;
import com.amazonaws.services.cloudwatch.model.EnableAlarmActionsRequest;
import com.amazonaws.services.cloudwatch.model.EnableAlarmActionsResult;
import com.amazonaws.services.cloudwatch.model.GetDashboardRequest;
import com.amazonaws.services.cloudwatch.model.GetDashboardResult;
import com.amazonaws.services.cloudwatch.model.GetMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricDataResult;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.cloudwatch.model.ListDashboardsRequest;
import com.amazonaws.services.cloudwatch.model.ListDashboardsResult;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.PutDashboardRequest;
import com.amazonaws.services.cloudwatch.model.PutDashboardResult;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmResult;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.SetAlarmStateRequest;
import com.amazonaws.services.cloudwatch.model.SetAlarmStateResult;
import com.amazonaws.services.cloudwatch.waiters.AmazonCloudWatchWaiters;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker.Builder;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDB.MTRecord;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDB.MTStreamDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/*
 * @author msgroi
 */
class MTAmazonDynamoDBStreamTestRunner {

    private static final Logger log = LoggerFactory.getLogger(MTAmazonDynamoDBTestRunner.class);
    private final AmazonDynamoDB mtAmazonDynamoDB;
    private StreamWorker streamWorker;
    private List<MTRecord> expectedMTRecords;

    MTAmazonDynamoDBStreamTestRunner(AmazonDynamoDB mtAmazonDynamoDB,
                                     AmazonDynamoDB rootAmazonDynamoDB,
                                     AmazonDynamoDBStreams rootAmazonDynamoDBStreams,
                                     List<MTRecord> expectedMTRecords) {
        this.mtAmazonDynamoDB = mtAmazonDynamoDB;
        if (rootAmazonDynamoDBStreams != null) {
            streamWorker = new StreamWorker(rootAmazonDynamoDB, rootAmazonDynamoDBStreams, expectedMTRecords.size());
            this.expectedMTRecords = new ArrayList<>(expectedMTRecords);
        }
    }

    void startStreamWorker() {
        if (streamWorker != null) {
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                    new StreamWorkerStarter(streamWorker, mtAmazonDynamoDB),
                    0,
                    1,
                    TimeUnit.SECONDS);
        }
    }

    void stop() {
        if (streamWorker != null) {
            streamWorker.stop();
        }
    }

    @SuppressWarnings("all")
    void await(int timeoutSeconds) {
        if (streamWorker != null) {
            streamWorker.await(timeoutSeconds);
            List<MTRecord> recordsReceived = new ArrayList<>(streamWorker.getRecordsReceived());
            assertEquals(expectedMTRecords.size(), recordsReceived.size(),
                        recordsReceived.size() + " of " + expectedMTRecords.size() + " records received");
            for (MTRecord recordReceived : recordsReceived) {
                assertMTRecord(recordReceived, expectedMTRecords);
            }
            assertEquals(0, expectedMTRecords.size(), "records not encountered: " + expectedMTRecords);
        }
    }

    private void assertMTRecord(MTRecord receivedRecord, List<MTRecord> expectedMTRecords) {
        Optional<MTRecord> mtRecordFound = expectedMTRecords.stream().filter(
                mtRecord -> new MTRecordComparator().compare(receivedRecord, mtRecord) == 0).findFirst();
        if (mtRecordFound.isPresent()) {
            expectedMTRecords.remove(mtRecordFound.get());
        } else {
            throw new IllegalArgumentException("unexpected MTRecord encountered: " + receivedRecord);
        }
    }

    private static class StreamWorkerStarter implements Runnable {

        private final AmazonDynamoDB mtAmazonDynamoDB;
        private final StreamWorker streamWorker;

        StreamWorkerStarter(StreamWorker streamWorker,
                            AmazonDynamoDB mtAmazonDynamoDB) {
            this.streamWorker = streamWorker;
            this.mtAmazonDynamoDB = mtAmazonDynamoDB;
        }

        @Override
        public void run() {
            ((MTAmazonDynamoDBBase) mtAmazonDynamoDB).listStreams(() -> new IRecordProcessor() {
                @Override
                public void initialize(InitializationInput initializationInput) {
                }
                @Override
                public void processRecords(ProcessRecordsInput processRecordsInput) {
                    processRecordsInput.getRecords().forEach(record -> {
                        MTRecord mtRecord = ((MTRecord)((RecordAdapter) record).getInternalObject());
                        streamWorker.recordReceived(mtRecord);
                    });
                }
                @Override
                public void shutdown(ShutdownInput shutdownInput) {}
            }).forEach(streamWorker::start);
        }

    }

    static class StreamWorker {
        private final ExecutorService workerPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        private final Map<String, Worker> workers = new HashMap<>();

        private final AmazonDynamoDB amazonDynamoDB;
        private final AmazonDynamoDBStreams amazonDynamoDBStreams;
        private final CountDownLatch countDownLatch;
        private final List<MTRecord> recordsReceived;

        StreamWorker(AmazonDynamoDB amazonDynamoDB,
                     AmazonDynamoDBStreams amazonDynamoDBStreams,
                     int expectedRecordCount) {
            this.amazonDynamoDB = amazonDynamoDB;
            this.amazonDynamoDBStreams = amazonDynamoDBStreams;
            this.countDownLatch = new CountDownLatch(expectedRecordCount);
            this.recordsReceived = new ArrayList<>();
        }

        private void start(MTStreamDescription streamDescription) {
            String streamArn = streamDescription.getArn();
            if (workers.get(streamArn) == null) {
                String applicationName = streamDescription.getLabel();
                Worker worker = new Builder()
                        .config(new KinesisClientLibConfiguration(
                                applicationName,
                                streamArn,
                                new DefaultAWSCredentialsProviderChain(),
                                applicationName + "_" + System.currentTimeMillis())
                                .withIdleTimeBetweenReadsInMillis(1)
                                .withCallProcessRecordsEvenForEmptyRecordList(true)
                                .withInitialLeaseTableReadCapacity(10)
                                .withInitialLeaseTableWriteCapacity(10)
                                .withTableName("oktodelete-LEASE_TABLE." + applicationName)
                                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON))
                        .recordProcessorFactory(streamDescription.getRecordProcessorFactory())
                        .dynamoDBClient(amazonDynamoDB)
                        .cloudWatchClient(new DummyCloudWatch())
                        .kinesisClient(new AmazonDynamoDBStreamsAdapterClient(amazonDynamoDBStreams))
                        .execService(workerPool)
                        .build();
                workerPool.submit(worker);
                workers.put(streamArn, worker);
                log.info("started stream listener on " + streamArn);
            }
        }

        private void stop() {
            workers.values().forEach(Worker::shutdown);
            workerPool.shutdown();
        }

        void recordReceived(MTRecord mtRecord) {
            this.recordsReceived.add(mtRecord);
            this.countDownLatch.countDown();
            log.info("record received, outstanding=" + countDownLatch.getCount() + ", record=" + mtRecord);
        }

        @SuppressWarnings("all")
        void await(int timeoutSeconds) {
            log.info("waiting " + timeoutSeconds + "s for " + this.countDownLatch.getCount() + " records to arrive ...");
            try {
                this.countDownLatch.await(timeoutSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {}
        }

        List<MTRecord> getRecordsReceived() {
            return recordsReceived;
        }

    }

    private static class MTRecordComparator implements Comparator<MTRecord> {

        @Override
        public int compare(MTRecord r1, MTRecord r2) {
            if (!r1.getContext().equals(r2.getContext())) return 1;
            if (!r1.getTableName().equals(r2.getTableName())) return 1;
            if (!r1.getEventName().equals(r2.getEventName())) return 1;
            if (!r1.getDynamodb().getKeys().equals(r2.getDynamodb().getKeys())) return 1;
            if ((r1.getDynamodb().getOldImage() == null && r2.getDynamodb().getOldImage() != null) ||
                    (r1.getDynamodb().getOldImage() != null && r2.getDynamodb().getOldImage() == null) || (
                    (r1.getDynamodb().getOldImage() != null && r2.getDynamodb().getOldImage() != null)
                            && !r1.getDynamodb().getOldImage().equals(r2.getDynamodb().getOldImage()))) return 1;
            if ((r1.getDynamodb().getNewImage() == null && r2.getDynamodb().getNewImage() != null) ||
                    (r1.getDynamodb().getNewImage() != null && r2.getDynamodb().getNewImage() == null) || (
                    (r1.getDynamodb().getNewImage() != null && r2.getDynamodb().getNewImage() != null)
                            && !r1.getDynamodb().getNewImage().equals(r2.getDynamodb().getNewImage()))) return 1;
            return 0;
        }
    }

    private static class DummyCloudWatch implements AmazonCloudWatch {
        @Override
        public void setEndpoint(String endpoint) {
        }

        @Override
        public void setRegion(Region region) {
        }

        @Override
        public DeleteAlarmsResult deleteAlarms(DeleteAlarmsRequest deleteAlarmsRequest) {
            return new DeleteAlarmsResult();
        }

        @Override
        public DeleteDashboardsResult deleteDashboards(DeleteDashboardsRequest deleteDashboardsRequest) {
            return new DeleteDashboardsResult();
        }

        @Override
        public DescribeAlarmHistoryResult describeAlarmHistory(
                DescribeAlarmHistoryRequest describeAlarmHistoryRequest) {
            return new DescribeAlarmHistoryResult();
        }

        @Override
        public DescribeAlarmHistoryResult describeAlarmHistory() {
            return new DescribeAlarmHistoryResult();
        }

        @Override
        public DescribeAlarmsResult describeAlarms(DescribeAlarmsRequest describeAlarmsRequest) {
            return new DescribeAlarmsResult();
        }

        @Override
        public DescribeAlarmsResult describeAlarms() {
            return new DescribeAlarmsResult();
        }

        @Override
        public DescribeAlarmsForMetricResult describeAlarmsForMetric(
                DescribeAlarmsForMetricRequest describeAlarmsForMetricRequest) {
            return new DescribeAlarmsForMetricResult();
        }

        @Override
        public DisableAlarmActionsResult disableAlarmActions(
                DisableAlarmActionsRequest disableAlarmActionsRequest) {
            return new DisableAlarmActionsResult();
        }

        @Override
        public EnableAlarmActionsResult enableAlarmActions(
                EnableAlarmActionsRequest enableAlarmActionsRequest) {
            return new EnableAlarmActionsResult();
        }

        @Override
        public GetDashboardResult getDashboard(GetDashboardRequest getDashboardRequest) {
            return new GetDashboardResult();
        }

        @Override
        public GetMetricDataResult getMetricData(GetMetricDataRequest getMetricDataRequest) {
            return new GetMetricDataResult();
        }

        @Override
        public GetMetricStatisticsResult getMetricStatistics(
                GetMetricStatisticsRequest getMetricStatisticsRequest) {
            return new GetMetricStatisticsResult();
        }

        @Override
        public ListDashboardsResult listDashboards(ListDashboardsRequest listDashboardsRequest) {
            return new ListDashboardsResult();
        }

        @Override
        public ListMetricsResult listMetrics(ListMetricsRequest listMetricsRequest) {
            return new ListMetricsResult();
        }

        @Override
        public ListMetricsResult listMetrics() {
            return new ListMetricsResult();
        }

        @Override
        public PutDashboardResult putDashboard(PutDashboardRequest putDashboardRequest) {
            return new PutDashboardResult();
        }

        @Override
        public PutMetricAlarmResult putMetricAlarm(PutMetricAlarmRequest putMetricAlarmRequest) {
            return new PutMetricAlarmResult();
        }

        @Override
        public PutMetricDataResult putMetricData(PutMetricDataRequest putMetricDataRequest) {
            return new PutMetricDataResult();
        }

        @Override
        public SetAlarmStateResult setAlarmState(SetAlarmStateRequest setAlarmStateRequest) {
            return new SetAlarmStateResult();
        }

        @Override
        public void shutdown() {
        }

        @Override
        public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
            return new ResponseMetadata(Collections.emptyMap());
        }

        @Override
        public AmazonCloudWatchWaiters waiters() {
            return new AmazonCloudWatchWaiters(null);
        }

    }

}