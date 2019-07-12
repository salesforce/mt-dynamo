package com.salesforce.dynamodbv2.mt.mappers;

import static com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.MT_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.dynamodbv2.testsupport.ArgumentBuilder.TestArgument;
import com.salesforce.dynamodbv2.testsupport.DefaultArgumentProvider;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests for {@link MtAmazonDynamoDb} api spec across implementations.
 */
public class MtAmazonDynamoDbTest {


    @ParameterizedTest(name = "{arguments}")
    @ArgumentsSource(DefaultArgumentProvider.class)
    public void testListTables_noMtContext(TestArgument testArgument) {
        MT_CONTEXT.setContext(null);
        List<String> allTables = testArgument.getAmazonDynamoDb().listTables().getTableNames();
        assertTrue(allTables.size() > 0);

        // test exclusive start key
        List<String> subset = testArgument.getAmazonDynamoDb().listTables(allTables.get(1)).getTableNames();
        assertEquals(allTables.size(), subset.size() + 2);

        // test pagination
        for (int i = 0; i < allTables.size() - 2; i++) {
            List<String> tables = testArgument.getAmazonDynamoDb().listTables(allTables.get(i), 2)
                .getTableNames();
            assertEquals(2, tables.size());
            assertEquals(tables.get(0), allTables.get(i + 1));
            assertEquals(tables.get(1), allTables.get(i + 2));
        }

    }
}
