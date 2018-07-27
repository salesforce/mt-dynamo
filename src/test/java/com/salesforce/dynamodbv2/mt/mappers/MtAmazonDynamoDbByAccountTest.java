/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers;

import static com.google.common.base.Preconditions.checkArgument;
import static com.salesforce.dynamodbv2.AmazonDynamoDbLocal.getNewAmazonDynamoDbLocal;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.context.impl.MtAmazonDynamoDbContextProviderImpl;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount.AmazonDynamoDbCache;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount.MtAccountCredentialsMapper;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount.MtAccountMapper;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount.MtAmazonDynamoDbByAccountBuilder;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDbByAccount.MtCredentialsBasedAmazonDynamoDbByAccountBuilder;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * This test requires that you have AWS credentials for 2 accounts, one default and one named 'personal' in your
 * ~/.aws/credentials file.  See http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html.
 *
 * @author msgroi
 */
public class MtAmazonDynamoDbByAccountTest {

    // local by default because hosted dynamo depends on hosted AWS, which is (1) slow and (2) requires two sets of
    // credentials.
    private static final boolean IS_LOCAL_DYNAMO = true;
    private static final AmazonDynamoDBClientBuilder AMAZON_DYNAMO_DB_CLIENT_BUILDER = AmazonDynamoDBClientBuilder
        .standard().withRegion(Regions.US_EAST_1);

    @Test
    void test() {
        MtAmazonDynamoDbContextProvider mtContext = new MtAmazonDynamoDbContextProviderImpl();
        if (IS_LOCAL_DYNAMO) {
            MtAmazonDynamoDbByAccountBuilder builder = MtAmazonDynamoDbByAccount.accountMapperBuilder()
                .withAccountMapper(LOCAL_DYNAMO_ACCOUNT_MAPPER)
                .withContext(mtContext);
            AmazonDynamoDB amazonDynamoDb = builder.build();
            new MtAmazonDynamoDbTestRunner(mtContext, amazonDynamoDb, amazonDynamoDb, null, false).runAll();
        } else {
            MtCredentialsBasedAmazonDynamoDbByAccountBuilder builder = MtAmazonDynamoDbByAccount.builder()
                .withAmazonDynamoDbClientBuilder(AMAZON_DYNAMO_DB_CLIENT_BUILDER)
                .withAccountCredentialsMapper(HOSTED_DYNAMO_ACCOUNT_MAPPER)
                .withContext(mtContext);
            AmazonDynamoDB amazonDynamoDb = builder.build();
            new MtAmazonDynamoDbTestRunner(mtContext, amazonDynamoDb, amazonDynamoDb, null, false).runAll();
        }
    }

    @Test
    void testCache() {
        AmazonDynamoDbCache cache = new AmazonDynamoDbCache();
        Function<String, AmazonDynamoDB> function = mock(Function.class);
        AmazonDynamoDB amazonDynamoDB1 = mock(AmazonDynamoDB.class);
        AmazonDynamoDB amazonDynamoDB2 = mock(AmazonDynamoDB.class);
        when(function.apply("ctx1")).thenReturn(amazonDynamoDB1);
        when(function.apply("ctx2")).thenReturn(amazonDynamoDB2);
        assertEquals(amazonDynamoDB1, cache.getAmazonDynamoDb("ctx1", function));
        assertEquals(amazonDynamoDB2, cache.getAmazonDynamoDb("ctx2", function));
        assertEquals(amazonDynamoDB1, cache.getAmazonDynamoDb("ctx1", function));
        assertEquals(amazonDynamoDB2, cache.getAmazonDynamoDb("ctx2", function));
        verify(function, times(2)).apply(any());
        verify(function, times(1)).apply("ctx1");
        verify(function, times(1)).apply("ctx2");
    }

    private static class TestAccountMapper implements MtAccountMapper, Supplier<Map<String, AmazonDynamoDB>> {

        private static final Map<String, AmazonDynamoDB> CACHE = ImmutableMap.of("ctx1", getNewAmazonDynamoDbLocal(),
            "ctx2", getNewAmazonDynamoDbLocal(),
            "ctx3", getNewAmazonDynamoDbLocal(),
            "ctx4", getNewAmazonDynamoDbLocal());

        @Override
        public AmazonDynamoDB getAmazonDynamoDb(MtAmazonDynamoDbContextProvider context) {
            checkArgument(CACHE.containsKey(context.getContext()), "invalid context '" + context + "'");
            return CACHE.get(context.getContext());
        }

        @Override
        public Map<String, AmazonDynamoDB> get() {
            return CACHE;
        }

    }

    private static class TestAccountCredentialsMapper implements MtAccountCredentialsMapper,
        Supplier<Map<String, AmazonDynamoDB>> {

        AWSCredentialsProvider ctx1CredentialsProvider = new ProfileCredentialsProvider();
        AWSCredentialsProvider ctx2CredentialsProvider = new ProfileCredentialsProvider("personal");
        AWSCredentialsProvider ctx3CredentialsProvider = new ProfileCredentialsProvider("scan1");
        AWSCredentialsProvider ctx4CredentialsProvider = new ProfileCredentialsProvider("scan2");

        @Override
        public AWSCredentialsProvider getAwsCredentialsProvider(String context) {
            switch (context) {
                case "1":
                    /*
                     * loads default profile
                     */
                    return ctx1CredentialsProvider;
                case "2":
                    /*
                     * loads 'personal' profile
                     *
                     * http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html
                     */
                    return ctx2CredentialsProvider;
                case "3":
                    return ctx3CredentialsProvider;
                case "4":
                    return ctx4CredentialsProvider;
                default:
                    throw new IllegalArgumentException("invalid context '" + context + "'");
            }
        }

        @Override
        public Map<String, AmazonDynamoDB> get() {
            return ImmutableMap.of("1",
                    AMAZON_DYNAMO_DB_CLIENT_BUILDER.withCredentials(ctx1CredentialsProvider).build(),
                "2", AMAZON_DYNAMO_DB_CLIENT_BUILDER.withCredentials(ctx2CredentialsProvider).build(),
                "3", AMAZON_DYNAMO_DB_CLIENT_BUILDER.withCredentials(ctx3CredentialsProvider).build(),
                "4", AMAZON_DYNAMO_DB_CLIENT_BUILDER.withCredentials(ctx4CredentialsProvider).build());
        }

    }

    public static final TestAccountMapper LOCAL_DYNAMO_ACCOUNT_MAPPER = new TestAccountMapper();
    public static final TestAccountCredentialsMapper HOSTED_DYNAMO_ACCOUNT_MAPPER = new TestAccountCredentialsMapper();

}