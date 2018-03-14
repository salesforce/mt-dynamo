/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * @author msgroi
 *
 * Allows for dividing tenants into different AWS accounts.  To use, pass in AmazonDynamoDBClientBuilder and MTAccountCredentialsMapper.
 * At run-time, a String representing the multi-tenant will be passed to your credentials mapper, allowing you map
 * the context to different AWS credentials implementations.
 *
 * MTAmazonDynamoDBByAccount does not support delegating to a mapper and therefore must always be at the end of the chain when it is used.
 *
 * To use, call the static builder() method.  The following parameters are required ...
 * - a multi-tenant context
 * - a MTAccountCredentialsMapper implementation that maps context to AWSCredentialsProvider's
 *
 * Supported:
 *    - methods: create|describe|delete Table, get|putItem, scan|query
 */
public class MTAmazonDynamoDBByAccount extends MTAmazonDynamoDBBase {

    public interface MTAccountCredentialsMapper {
        AWSCredentialsProvider getAWSCredentialsProvider(String context);
    }

    public static MTCredentialsBasedAmazonDynamoDBByAccountBuilder builder() {
        return new MTCredentialsBasedAmazonDynamoDBByAccountBuilder();
    }

    public static class MTCredentialsBasedAmazonDynamoDBByAccountBuilder {
        private MTAmazonDynamoDBContextProvider mtContext;
        private AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder;
        private MTAccountCredentialsMapper credentialsMapper;

        public MTCredentialsBasedAmazonDynamoDBByAccountBuilder withContext(MTAmazonDynamoDBContextProvider mtContext) {
            this.mtContext = mtContext;
            return this;
        }

        public MTCredentialsBasedAmazonDynamoDBByAccountBuilder withAmazonDynamoDBClientBuilder(AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder) {
            this.amazonDynamoDBClientBuilder = amazonDynamoDBClientBuilder;
            return this;
        }

        public MTCredentialsBasedAmazonDynamoDBByAccountBuilder withAccountCredentialsMapper(MTAccountCredentialsMapper credentialsMapper) {
            this.credentialsMapper = credentialsMapper;
            return this;
        }

        public AmazonDynamoDB build() {
            Preconditions.checkNotNull(mtContext, "mtContext is required");
            Preconditions.checkNotNull(amazonDynamoDBClientBuilder, "amazonDynamoDBClientBuilder is required");
            Preconditions.checkNotNull(credentialsMapper,"credentialsMapper is required");
            return new MTAmazonDynamoDBByAccount(mtContext, amazonDynamoDBClientBuilder, credentialsMapper);
        }

    }

    @Override
    public AmazonDynamoDB getAmazonDynamoDB() {
        return accountMapper.getAmazonDynamoDB(getMTContext());
    }

    @VisibleForTesting
    static class AmazonDynamoDBCache {
        final ConcurrentHashMap<String, AmazonDynamoDB> cache = new ConcurrentHashMap<>();
        AmazonDynamoDB getAmazonDynamoDB(String mtContext, Function<String, AmazonDynamoDB> amazonDynamoDBCreator) {
            return cache.computeIfAbsent(mtContext, amazonDynamoDBCreator);
        }
    }

    /*
     * Everything that is @VisibleForTesting below is exposed to allow MTAmazonDynamoDBChain to be tested without
     * connecting to AWS-hosted DynamoDB.
     */
    @VisibleForTesting
    public static MTAmazonDynamoDBByAccountBuilder accountMapperBuilder() {
        return new MTAmazonDynamoDBByAccountBuilder();
    }

    /*
     * Takes a context provider and returns an AmazonDynamoDB to be used to store tenant data.
     */
    @VisibleForTesting
    interface MTAccountMapper {
        AmazonDynamoDB getAmazonDynamoDB(MTAmazonDynamoDBContextProvider mtContext);
    }

    @VisibleForTesting
    public static class MTAmazonDynamoDBByAccountBuilder {
        private MTAmazonDynamoDBContextProvider mtContext;
        private MTAccountMapper accountMapper;

        public MTAmazonDynamoDBByAccountBuilder withContext(MTAmazonDynamoDBContextProvider mtContext) {
            this.mtContext = mtContext;
            return this;
        }

        public MTAmazonDynamoDBByAccountBuilder withAccountMapper(MTAccountMapper accountMapper) {
            this.accountMapper = accountMapper;
            return this;
        }

        public AmazonDynamoDB build() {
            Preconditions.checkNotNull(mtContext, "mtContext is required");
            Preconditions.checkNotNull(accountMapper, "accountMapper is required");
            return new MTAmazonDynamoDBByAccount(mtContext, accountMapper);
        }
    }

    private final MTAccountMapper accountMapper;

    private MTAmazonDynamoDBByAccount(MTAmazonDynamoDBContextProvider mtContext,
                                      MTAccountMapper accountMapper) {
        super(mtContext, null);
        this.accountMapper = accountMapper;
    }

    private MTAmazonDynamoDBByAccount(MTAmazonDynamoDBContextProvider mtContext,
                                      AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder,
                                      MTAccountCredentialsMapper credentialsMapper) {
        super(mtContext, null);
        this.accountMapper = new CredentialBasedAccountMapperImpl(amazonDynamoDBClientBuilder, credentialsMapper);
    }

    @Override
    public List<MTStreamDescription> listStreams(IRecordProcessorFactory factory) {
        throw new UnsupportedOperationException();
    }

    /**
     * Default implementation of MTAccountMapper delegates to a MTAccountCredentialsMapper which returns an
     * AWSCredentialsProvider for the AWS account to be used to store tenant data.
     */
    private static class CredentialBasedAccountMapperImpl implements MTAccountMapper {

        private static final AmazonDynamoDBCache cache = new AmazonDynamoDBCache();
        private final AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder;
        private final MTAccountCredentialsMapper credentialsMapper;

        CredentialBasedAccountMapperImpl(AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder,
                                         MTAccountCredentialsMapper credentialsMapper) {
            this.amazonDynamoDBClientBuilder = amazonDynamoDBClientBuilder;
            this.credentialsMapper = credentialsMapper;
        }

        public AmazonDynamoDB getAmazonDynamoDB(MTAmazonDynamoDBContextProvider mtContext) {
            return cache.getAmazonDynamoDB(
                    mtContext.getContext(),
                    context -> amazonDynamoDBClientBuilder.withCredentials(credentialsMapper.getAWSCredentialsProvider(context)).build());
        }

    }

}