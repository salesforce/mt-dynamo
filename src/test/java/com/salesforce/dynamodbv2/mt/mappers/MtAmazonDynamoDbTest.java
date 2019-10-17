package com.salesforce.dynamodbv2.mt.mappers;

import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE1;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE2;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE3;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE4;
import static com.salesforce.dynamodbv2.testsupport.DefaultTestSetup.TABLE5;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider.DefaultArgumentProviderConfig;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests for {@link MtAmazonDynamoDb} API spec across implementations.
 */
public class MtAmazonDynamoDbTest {

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1, TABLE2, TABLE3, TABLE4, TABLE5})
    public void testListTables_noContext(TestArgument testArgument) {
        MT_CONTEXT.setContext(null);
        final List<String> allTables = testArgument.getAmazonDynamoDb().listTables().getTableNames();

        assertFalse(allTables.isEmpty());
    }

    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    @DefaultArgumentProviderConfig(tables = {TABLE1, TABLE2, TABLE3, TABLE4, TABLE5})
    public void testListTables_noContext_pagination(TestArgument testArgument) {
        MT_CONTEXT.setContext(null);
        final Set<String> allTableSet = new HashSet<>();
        int lastBatchSize = 0;
        final int limit = 5;
        String exclusiveStartKey = null;
        do {
            final ListTablesResult listTablesResult = testArgument.getAmazonDynamoDb()
                .listTables(exclusiveStartKey, limit);
            assertTrue(!listTablesResult.getTableNames().isEmpty() || lastBatchSize == limit,
                "Should have received non-empty table set or last result set should have been full");
            lastBatchSize = listTablesResult.getTableNames().size();
            final int sizeBefore = allTableSet.size();
            allTableSet.addAll(listTablesResult.getTableNames());
            assertEquals(sizeBefore + lastBatchSize, allTableSet.size(),
                "duplicate values detecting paging through--all values should be unique");
            exclusiveStartKey = listTablesResult.getLastEvaluatedTableName();
        } while (exclusiveStartKey != null);
    }
}
