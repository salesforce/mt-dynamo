package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.auth.AWSCredentialsProvider;
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
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker.Builder;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtStreamDescription;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts a DynamoDBStreams listener.
 *
 * @author msgroi
 */
public class StreamWorker {

    private static final Logger log = LoggerFactory.getLogger(StreamWorker.class);
    private final ExecutorService workerPool = Executors
        .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    private final Map<String, Worker> workers = new HashMap<>();

    private final AmazonDynamoDB amazonDynamoDb;
    private final AmazonDynamoDBStreams amazonDynamoDbStreams;
    private final AWSCredentialsProvider awsCredentialsProvider;

    /**
     * Constructor.
     */
    public StreamWorker(AmazonDynamoDB amazonDynamoDb,
        AmazonDynamoDBStreams amazonDynamoDbStreams,
        AWSCredentialsProvider awsCredentialsProvider) {
        this.amazonDynamoDb = amazonDynamoDb;
        this.amazonDynamoDbStreams = amazonDynamoDbStreams;
        this.awsCredentialsProvider = awsCredentialsProvider;
    }

    /**
     * Takes a stream descriptions arn, label, and record processor and starts a stream listener.
     */
    public void start(MtStreamDescription streamDescription) {
        String streamArn = streamDescription.getArn();
        if (workers.get(streamArn) == null) {
            String applicationName = streamDescription.getLabel();
            Worker worker = new Builder()
                .config(new KinesisClientLibConfiguration(
                    applicationName,
                    streamArn,
                    awsCredentialsProvider,
                    applicationName + "_" + System.currentTimeMillis())
                    .withIdleTimeBetweenReadsInMillis(1)
                    .withCallProcessRecordsEvenForEmptyRecordList(true)
                    .withInitialLeaseTableReadCapacity(10)
                    .withInitialLeaseTableWriteCapacity(10)
                    .withTableName("oktodelete-LEASE_TABLE." + applicationName)
                    .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON))
                .recordProcessorFactory(streamDescription.getRecordProcessorFactory())
                .dynamoDBClient(amazonDynamoDb)
                .cloudWatchClient(new DummyCloudWatch())
                .kinesisClient(new AmazonDynamoDBStreamsAdapterClient(amazonDynamoDbStreams))
                .execService(workerPool)
                .build();
            workerPool.submit(worker);
            workers.put(streamArn, worker);
            log.info("started stream listener on " + streamArn);
        }
    }

    public void stop() {
        workers.values().forEach(Worker::shutdown);
    }

    private static class DummyCloudWatch implements AmazonCloudWatch {
        @Override
        @Deprecated
        public void setEndpoint(String endpoint) {
        }

        @Override
        @Deprecated
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