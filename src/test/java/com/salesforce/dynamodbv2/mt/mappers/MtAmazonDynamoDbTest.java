package com.salesforce.dynamodbv2.mt.mappers;

import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.google.common.collect.Lists;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.internal.util.collections.Sets;

/**
 * Tests for {@link MtAmazonDynamoDb} api spec across implementations.
 */
public class MtAmazonDynamoDbTest {


    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    public void testListTables_noContext(TestArgument testArgument) {
        MT_CONTEXT.setContext(null);
        List<String> allTables = testArgument.getAmazonDynamoDb().listTables().getTableNames();
        assertTrue(allTables.size() > 0);
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    public void testListTables_noContext_pagination(TestArgument testArgument) {
        MT_CONTEXT.setContext(null);
        List<String> allTables = Lists.newArrayList();
        Set<String> allTableSet = new HashSet<>();
        ListTablesResult listTablesResult;
        String exclusiveStartKey = null;
        int lastBatchSize = 0;
        int limit = 5;
        do {
            listTablesResult = testArgument.getAmazonDynamoDb().listTables(exclusiveStartKey, limit);
            assertTrue(listTablesResult.getTableNames().size() > 0 || lastBatchSize == limit,
                "Should not have gotten empty table set, or last result set should have been full");
            int sizeBefore = allTableSet.size();
            lastBatchSize = listTablesResult.getTableNames().size();
            allTables.addAll(listTablesResult.getTableNames());
            allTableSet.addAll(listTablesResult.getTableNames());
            assertEquals(sizeBefore + lastBatchSize, allTableSet.size(),
                "dupe values detecting paging through, all values should be unique");
            exclusiveStartKey = listTablesResult.getLastEvaluatedTableName();
        } while (exclusiveStartKey != null);
    }
}
