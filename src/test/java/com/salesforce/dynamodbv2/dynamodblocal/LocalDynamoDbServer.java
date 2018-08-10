package com.salesforce.dynamodbv2.dynamodblocal;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import java.io.IOException;
import java.net.ServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts up a DynamoDB server using the command line launcher and returns an AmazonDynamoDB client that
 * can connect to it via a local network port.
 *
 * @author msgroi
 */
public class LocalDynamoDbServer {

    private static final Logger log = LoggerFactory.getLogger(LocalDynamoDbServer.class);
    private DynamoDBProxyServer server;
    private int port;
    private boolean running;

    public LocalDynamoDbServer() {
        this.port = getRandomPort();
    }

    /**
     * Find a random open port and returns it.
     */
    public static int getRandomPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Creates an instance with the specified port.
     */
    public LocalDynamoDbServer(int port) {
        this.port = port;
    }

    /**
     * If it's not already running, starts the server, then returns a client regardless.
     */
    public AmazonDynamoDB start() {
        if (!running) {
            try {
                System.setProperty("sqlite4java.library.path", "src/test/resources/bin");
                server = ServerRunner
                    .createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", String.valueOf(port)});
                server.start();
                running = true;
                log.info("started dynamodblocal on port " + port);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return getClient();
    }

    /**
     * If the server is running, stops the server.  Returns silently otherwise.
     */
    public void stop() {
        if (running) {
            try {
                server.stop();
                running = false;
                log.info("stopped dynamodblocal on port " + port);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /*
     * Returns the port of the server.
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns a client that can be used to connect to the server.
     */
    public AmazonDynamoDB getClient() {
        return AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:" + port, null))
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""))).build();
    }

}