package com.salesforce.dynamodbv2.mt.mappers;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.amazonaws.services.dynamodbv2.model.StreamViewType.NEW_AND_OLD_IMAGES;
import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.common.collect.ImmutableMap;
import com.salesforce.dynamodbv2.dynamodblocal.AmazonDynamoDbLocal;
import com.salesforce.dynamodbv2.mt.mappers.MtAmazonDynamoDb.MtRecord;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.SharedTableBuilder;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbBySharedTable;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl.MtAmazonDynamoDbStreamsBySharedTable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MtAmazonDynamoDbStreamsTest {

    /**
     * Verifies that getting a streams client for a non-multi-tenant dynamo instance throws an exception..
     */
    @Test
    void testCreateFromDynamoNotMultitenant() {
        assertThrows(IllegalArgumentException.class, () -> MtAmazonDynamoDbStreams
            .createFromDynamo(mock(AmazonDynamoDB.class), mock(AmazonDynamoDBStreams.class)));
    }

    /**
     * Verifies that getting a streams client for an instance of {@link MtAmazonDynamoDbByTable} yields a pass-through
     * instance of {@link MtAmazonDynamoDbStreams}.
     */
    @Test
    void testCreateFromDynamoByTable() {
        AmazonDynamoDBStreams actual = MtAmazonDynamoDbStreams
            .createFromDynamo(mock(MtAmazonDynamoDbByTable.class), mock(AmazonDynamoDBStreams.class));

        assertTrue(actual instanceof MtAmazonDynamoDbStreamsByTable,
            "Expected an instance of MtAmazonDynamoDbStreamsPassthrough");
    }

    @Test
    void testCreateFromDynamoByAccount() {
        // TODO Not implemented yet
        assertThrows(UnsupportedOperationException.class, () -> MtAmazonDynamoDbStreams
            .createFromDynamo(mock(MtAmazonDynamoDbByAccount.class), mock(AmazonDynamoDBStreams.class)));
    }

    @Test
    void testCreateFromDynamoByIndex() {
        AmazonDynamoDBStreams actual = MtAmazonDynamoDbStreams
            .createFromDynamo(mock(MtAmazonDynamoDbBySharedTable.class), mock(AmazonDynamoDBStreams.class));

        assertTrue(actual instanceof MtAmazonDynamoDbStreamsBySharedTable,
            "Expected an instance of MtAmazonDynamoDbStreamsBySharedTable");
    }

}