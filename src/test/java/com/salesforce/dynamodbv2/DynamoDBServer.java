package com.salesforce.dynamodbv2;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;

/**
 * @author msgroi
 */
class DynamoDBServer {

    private DynamoDBProxyServer server;
    private int port;
    private boolean running;

    DynamoDBServer(int port) {
        this.port = port;
    }

    AmazonDynamoDB start() {
        if (!running) {
            try {
                System.setProperty("sqlite4java.library.path", "src/test/resources/bin");
                server = ServerRunner
                    .createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", String.valueOf(port)});
                server.start();
                running = true;
                System.out.println("started dynamodblocal on port " + port);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return getClient();
    }

    void stop() {
        if (running) {
            try {
                server.stop();
                running = false;
                System.out.println("stopped dynamodblocal on port " + port);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private AmazonDynamoDB getClient() {
        return AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:" + port, null))
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""))).build();
    }
}