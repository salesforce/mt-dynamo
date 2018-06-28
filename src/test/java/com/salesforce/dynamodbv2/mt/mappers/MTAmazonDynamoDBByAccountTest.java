/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MTAmazonDynamoDBContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MTAmazonDynamoDBContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByAccount.AmazonDynamoDBCache;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByAccount.MTAccountCredentialsMapper;
import com.salesforce.dynamodbv2.mt.mappers.MTAmazonDynamoDBByAccount.MTAccountMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.salesforce.dynamodbv2.AmazonDynamoDBLocal.getNewAmazonDynamoDBLocal;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author msgroi
 *
 * This test requires that you have AWS credentials for 2 accounts, one default and one named 'personal' in
 * your ~/.aws/credentials file.  See http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html.
 */
public class MTAmazonDynamoDBByAccountTest {

    // local by default because hosted dynamo depends on hosted AWS which is 1) slow, and 2) requires two sets of credentials.
    private static final boolean isLocalDynamo = true;
    private static AmazonDynamoDBClientBuilder amazonDynamoDBClientBuilder = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1);

    @Test
    void test() {
        MTAmazonDynamoDBContextProvider mtContext = new MTAmazonDynamoDBContextProviderImpl();
        if (isLocalDynamo) {
            MTAmazonDynamoDBByAccount.MTAmazonDynamoDBByAccountBuilder builder = MTAmazonDynamoDBByAccount.accountMapperBuilder()
                    .withAccountMapper(LOCAL_DYNAMO_ACCOUNT_MAPPER)
                    .withContext(mtContext);
            AmazonDynamoDB amazonDynamoDB = builder.build();
            new MTAmazonDynamoDBTestRunner(mtContext, amazonDynamoDB, amazonDynamoDB,
                    false).runAll();
        } else {
            MTAmazonDynamoDBByAccount.MTCredentialsBasedAmazonDynamoDBByAccountBuilder builder = MTAmazonDynamoDBByAccount.builder()
                    .withAmazonDynamoDBClientBuilder(amazonDynamoDBClientBuilder)
                    .withAccountCredentialsMapper(HOSTED_DYNAMO_ACCOUNT_MAPPER)
                    .withContext(mtContext);
            AmazonDynamoDB amazonDynamoDB = builder.build();
            new MTAmazonDynamoDBTestRunner(mtContext, amazonDynamoDB, amazonDynamoDB,
                    false).runAll();
        }
    }

    @Test
    void testCache() {
        AmazonDynamoDBCache cache = new AmazonDynamoDBCache();
        Function<String, AmazonDynamoDB> function = mock(Function.class);
        AmazonDynamoDB amazonDynamoDB1 = mock(AmazonDynamoDB.class);
        AmazonDynamoDB amazonDynamoDB2 = mock(AmazonDynamoDB.class);
        when(function.apply("ctx1")).thenReturn(amazonDynamoDB1);
        when(function.apply("ctx2")).thenReturn(amazonDynamoDB2);
        assertEquals(amazonDynamoDB1, cache.getAmazonDynamoDB("ctx1", function));
        assertEquals(amazonDynamoDB2, cache.getAmazonDynamoDB("ctx2", function));
        assertEquals(amazonDynamoDB1, cache.getAmazonDynamoDB("ctx1", function));
        assertEquals(amazonDynamoDB2, cache.getAmazonDynamoDB("ctx2", function));
        verify(function, times(2)).apply(any());
        verify(function, times(1)).apply("ctx1");
        verify(function, times(1)).apply("ctx2");
    }

    private static class TestAccountMapper implements MTAccountMapper, Supplier<Map<String, AmazonDynamoDB>> {

        private static Map<String, AmazonDynamoDB> cache = ImmutableMap.of("ctx1", getNewAmazonDynamoDBLocal(),
                                                                           "ctx2", getNewAmazonDynamoDBLocal());

        @Override
        public AmazonDynamoDB getAmazonDynamoDB(MTAmazonDynamoDBContextProvider context) {
            checkArgument(cache.containsKey(context.getContext()), "invalid context '" + context + "'");
            return cache.get(context.getContext());
        }

        @Override
        public Map<String, AmazonDynamoDB> get() {
            return cache;
        }

    }

    private static class TestAccountCredentialsMapper implements MTAccountCredentialsMapper, Supplier<Map<String, AmazonDynamoDB>> {

        AWSCredentialsProvider ctx1CredentialsProvider = new ProfileCredentialsProvider();
        AWSCredentialsProvider ctx2CredentialsProvider = new ProfileCredentialsProvider("personal");

        @Override
        public AWSCredentialsProvider getAWSCredentialsProvider(String context) {
            if (context.equals("1")) {
                /*
                 * loads default profile
                 */
                return ctx1CredentialsProvider;
            } else {
                if (context.equals("2")) {
                    /*
                     * loads 'personal' profile
                     *
                     * http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html
                     */
                    return ctx2CredentialsProvider;
                } else {
                    throw new IllegalArgumentException("invalid context '" + context + "'");
                }
            }
        }

        @Override
        public Map<String, AmazonDynamoDB> get() {
            return ImmutableMap.of("1", amazonDynamoDBClientBuilder.withCredentials(ctx1CredentialsProvider).build(),
                                   "2", amazonDynamoDBClientBuilder.withCredentials(ctx2CredentialsProvider).build());
        }

    }

    public static final TestAccountMapper LOCAL_DYNAMO_ACCOUNT_MAPPER = new TestAccountMapper();
    public static final TestAccountCredentialsMapper HOSTED_DYNAMO_ACCOUNT_MAPPER = new TestAccountCredentialsMapper();

}