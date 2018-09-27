package com.salesforce.dynamodbv2.testsupport;

import static com.salesforce.dynamodbv2.dynamodblocal.LocalDynamoDbServer.getRandomPort;
import static com.salesforce.dynamodbv2.testsupport.TestSupport.IS_LOCAL_DYNAMO;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.salesforce.dynamodbv2.dynamodblocal.LocalDynamoDbServer;

/**
 * Run with an independent, isolated DynamoDB instance when run locally.  Also disables all table setup but still feeds
 * {@code TestArgument}s as supplied by the {@code ArgumentBuilder}.  DynamoDB will start up as an external process that
 * can be accessed via the local network.  It picks an open port randomly, then will restart the DynamoDB server process
 * between each test run, effectively dropping the database.
 *
 * <p>Note that tests that running such tests in the same surefire run as other tests that use the default
 * locally networked DynamoDB typically fail with sqlite related errors.  The workaround is to run with these tests
 * with forkCount=1/reuseForks=false.  There are existing tests set up this way and are annotated with
 * {@code @Tag("isolated-tests")}.  The default surefire configuration excludes tests with this annotation.  There is a
 * separate Maven profile, 'isolated-tests', that runs just these tests.
 *
 * @author msgroi
 */
public class IsolatedArgumentProvider extends DefaultArgumentProvider {

    private static AmazonDynamoDB amazonDynamoDb;
    private static AmazonDynamoDBStreams amazonDynamoDbStreams;
    private static LocalDynamoDbServer server;
    private static boolean initialized = false;
    private static int port = getRandomPort();

    public IsolatedArgumentProvider(TestSetup testSetup) {
        super(new ArgumentBuilder(amazonDynamoDb), testSetup);
    }

    private static void initializeDynamoDbClients() {
        if (!initialized) {
            if (IS_LOCAL_DYNAMO) {
                /*
                 * Normally don't like to commit commented out code but this may come in handy as it allows for
                 * running DynamoDB in embedded mode as opposed to locally via a network port.
                 */
                // if (USE_EMBEDDED_DYNAMO) {
                //     DynamoDbClients localDynamoDbClients =
                //     AmazonDynamoDbLocal.getNewAmazonDynamoDbLocalWithStreams();
                //     amazonDynamoDb = localDynamoDbClients.getAndInitializeAmazonDynamoDb();
                //     amazonDynamoDbStreams = localDynamoDbClients.getAndInitializeAmazonDynamoDbStreams();
                // } else {
                server = new LocalDynamoDbServer(port);
                amazonDynamoDb = server.start();
                amazonDynamoDbStreams = AmazonDynamoDBStreamsClientBuilder.standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "http://localhost:" + server.getPort(), null))
                    .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials("", ""))).build();
                // }
            } else {
                amazonDynamoDb = AmazonDynamoDBClientBuilder.standard()
                    .withRegion(ArgumentBuilder.REGION).build();
                amazonDynamoDbStreams = AmazonDynamoDBStreamsClientBuilder.standard()
                    .withRegion(ArgumentBuilder.REGION).build();
            }
            initialized = true;
        }
    }

    public static AmazonDynamoDB getAndInitializeAmazonDynamoDb() {
        initializeDynamoDbClients();
        return amazonDynamoDb;
    }

    public static AmazonDynamoDBStreams getAndInitializeAmazonDynamoDbStreams() {
        initializeDynamoDbClients();
        return amazonDynamoDbStreams;
    }

    /**
     * Shuts down the server.
     */
    public static void shutdown() {
        if (server != null) {
            server.stop();
        }
        initialized = false;
    }

}