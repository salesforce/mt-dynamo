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
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Allows for dividing tenants into different AWS accounts.  To use, pass in AmazonDynamoDBClientBuilder and
 * MtAccountCredentialsMapper.  At run-time, a String representing the multi-tenant will be passed to your
 * credentials mapper, allowing you map the context to different AWS credentials implementations.
 *
 * <p>MtAmazonDynamoDbByAccount does not support delegating to a mapper and therefore must always be at the end
 * of the chain when it is used.
 *
 * <p>To use, call the static builder() method.  The following parameters are required ...
 * - a multi-tenant context
 * - a MtAccountCredentialsMapper implementation that maps context to AWSCredentialsProvider's
 *
 * <p>Supported:
 * - methods: batchGet|get|put Item, create|describe|delete Table, scan|query
 *
 * @author msgroi
 */
public class MtAmazonDynamoDbByAccount extends MtAmazonDynamoDbBase {

    public interface MtAccountCredentialsMapper {
        AWSCredentialsProvider getAwsCredentialsProvider(String context);
    }

    public static MtCredentialsBasedAmazonDynamoDbByAccountBuilder builder() {
        return new MtCredentialsBasedAmazonDynamoDbByAccountBuilder();
    }

    public static class MtCredentialsBasedAmazonDynamoDbByAccountBuilder {
        private MtAmazonDynamoDbContextProvider mtContext;
        private AmazonDynamoDBClientBuilder amazonDynamoDbClientBuilder;
        private MtAccountCredentialsMapper credentialsMapper;

        public MtCredentialsBasedAmazonDynamoDbByAccountBuilder withContext(MtAmazonDynamoDbContextProvider mtContext) {
            this.mtContext = mtContext;
            return this;
        }

        public MtCredentialsBasedAmazonDynamoDbByAccountBuilder withAmazonDynamoDbClientBuilder(
            AmazonDynamoDBClientBuilder amazonDynamoDbClientBuilder) {
            this.amazonDynamoDbClientBuilder = amazonDynamoDbClientBuilder;
            return this;
        }

        public MtCredentialsBasedAmazonDynamoDbByAccountBuilder withAccountCredentialsMapper(
            MtAccountCredentialsMapper credentialsMapper) {
            this.credentialsMapper = credentialsMapper;
            return this;
        }

        /**
         * TODO: write Javadoc.
         *
         * @return a newly created {@code AmazonDynamoDB} based on the contents of the
         *     {@code MtCredentialsBasedAmazonDynamoDbByAccountBuilder}
         */
        public AmazonDynamoDB build() {
            Preconditions.checkNotNull(mtContext, "mtContext is required");
            Preconditions.checkNotNull(amazonDynamoDbClientBuilder,
                "amazonDynamoDbClientBuilder is required");
            Preconditions.checkNotNull(credentialsMapper, "credentialsMapper is required");
            return new MtAmazonDynamoDbByAccount(mtContext, amazonDynamoDbClientBuilder, credentialsMapper);
        }

    }

    @Override
    public AmazonDynamoDB getAmazonDynamoDb() {
        return accountMapper.getAmazonDynamoDb(getMtContext());
    }

    @Override
    public void shutdown() {
        super.shutdown();
        accountMapper.shutdown();
    }

    @VisibleForTesting
    static class AmazonDynamoDbCache {
        final ConcurrentHashMap<String, AmazonDynamoDB> cache = new ConcurrentHashMap<>();

        AmazonDynamoDB getAmazonDynamoDb(String mtContext, Function<String, AmazonDynamoDB> amazonDynamoDbCreator) {
            return cache.computeIfAbsent(mtContext, amazonDynamoDbCreator);
        }

        void invalidateCaches() {
            cache.clear();
        }
    }

    /*
     * Everything that is @VisibleForTesting below is exposed to be able to run tests without
     * connecting to AWS-hosted DynamoDB.
     */
    @VisibleForTesting
    public static MtAmazonDynamoDbByAccountBuilder accountMapperBuilder() {
        return new MtAmazonDynamoDbByAccountBuilder();
    }

    /*
     * Takes a context provider and returns an AmazonDynamoDB to be used to store tenant data.
     */
    @VisibleForTesting
    public interface MtAccountMapper {

        AmazonDynamoDB getAmazonDynamoDb(MtAmazonDynamoDbContextProvider mtContext);

        default void shutdown() {}

    }

    @VisibleForTesting
    public static class MtAmazonDynamoDbByAccountBuilder {
        private MtAmazonDynamoDbContextProvider mtContext;
        private MtAccountMapper accountMapper;

        public MtAmazonDynamoDbByAccountBuilder withContext(MtAmazonDynamoDbContextProvider mtContext) {
            this.mtContext = mtContext;
            return this;
        }

        public MtAmazonDynamoDbByAccountBuilder withAccountMapper(MtAccountMapper accountMapper) {
            this.accountMapper = accountMapper;
            return this;
        }

        /**
         * TODO: write Javadoc.
         *
         * @return a newly created {@code AmazonDynamoDB} based on the contents of the
         *     {@code MtAmazonDynamoDbByAccountBuilder}
         */
        public AmazonDynamoDB build() {
            Preconditions.checkNotNull(mtContext, "mtContext is required");
            Preconditions.checkNotNull(accountMapper, "accountMapper is required");
            return new MtAmazonDynamoDbByAccount(mtContext, accountMapper);
        }
    }

    private final MtAccountMapper accountMapper;

    private MtAmazonDynamoDbByAccount(MtAmazonDynamoDbContextProvider mtContext,
                                      MtAccountMapper accountMapper) {
        super(mtContext, null);
        this.accountMapper = accountMapper;
    }

    private MtAmazonDynamoDbByAccount(MtAmazonDynamoDbContextProvider mtContext,
                                      AmazonDynamoDBClientBuilder amazonDynamoDbClientBuilder,
                                      MtAccountCredentialsMapper credentialsMapper) {
        super(mtContext, null);
        this.accountMapper = new CredentialBasedAccountMapperImpl(amazonDynamoDbClientBuilder, credentialsMapper);
    }

    @Override
    public List<MtStreamDescription> listStreams(IRecordProcessorFactory factory) {
        throw new UnsupportedOperationException();
    }

    /**
     * Default implementation of MtAccountMapper delegates to a MtAccountCredentialsMapper which returns an
     * AWSCredentialsProvider for the AWS account to be used to store tenant data.
     */
    private static class CredentialBasedAccountMapperImpl implements MtAccountMapper {

        private static final AmazonDynamoDbCache CACHE = new AmazonDynamoDbCache();
        private final AmazonDynamoDBClientBuilder amazonDynamoDbClientBuilder;
        private final MtAccountCredentialsMapper credentialsMapper;

        CredentialBasedAccountMapperImpl(AmazonDynamoDBClientBuilder amazonDynamoDbClientBuilder,
                                         MtAccountCredentialsMapper credentialsMapper) {
            this.amazonDynamoDbClientBuilder = amazonDynamoDbClientBuilder;
            this.credentialsMapper = credentialsMapper;
        }

        public AmazonDynamoDB getAmazonDynamoDb(MtAmazonDynamoDbContextProvider mtContext) {
            return CACHE.getAmazonDynamoDb(
                mtContext.getContext(),
                context -> amazonDynamoDbClientBuilder.withCredentials(
                    credentialsMapper.getAwsCredentialsProvider(context)).build());
        }

    }

    @Override
    public void invalidateCaches() {
        CredentialBasedAccountMapperImpl.CACHE.invalidateCaches();
    }

}