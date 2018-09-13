package com.salesforce.dynamodbv2.mt.mappers.sharedtable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.google.common.collect.ImmutableList;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.HybridSharedTableBuilder.CreateTableRequestFactoryEnsemble;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests HybridSharedTableBuilder.
 *
 * @author msgroi
 */
class HybridSharedTableBuilderTest {

    private static DynamoTableDescription virtualTable1 = mock(DynamoTableDescription.class);
    private static CreateTableRequest createTableRequest1 = mock(CreateTableRequest.class);
    private static CreateTableRequestFactory ctrf1 = mock(CreateTableRequestFactory.class);
    private static DynamoTableDescription virtualTable2 = mock(DynamoTableDescription.class);
    private static CreateTableRequest createTableRequest2 = mock(CreateTableRequest.class);
    private static CreateTableRequestFactory ctrf2 = mock(CreateTableRequestFactory.class);
    private static DynamoTableDescription virtualTable3 = mock(DynamoTableDescription.class);

    @BeforeAll
    static void beforeAll() {
        when(ctrf1.getCreateTableRequest(virtualTable1)).thenReturn(Optional.of(createTableRequest1));
        when(ctrf2.getCreateTableRequest(virtualTable2)).thenReturn(Optional.of(createTableRequest2));
        when(ctrf1.getPhysicalTables()).thenReturn(ImmutableList.of(createTableRequest1));
        when(ctrf2.getPhysicalTables()).thenReturn(ImmutableList.of(createTableRequest2));
    }

    @Test
    void testIteratingCreateTableRequestFactory_getCreateTableRequest() {
        CreateTableRequestFactoryEnsemble sut = new CreateTableRequestFactoryEnsemble(ImmutableList.of(ctrf1, ctrf2));
        // resolved by ctrf1
        assertEquals(createTableRequest1, sut.getCreateTableRequest(virtualTable1).get());
        // resolved by ctrf2
        assertEquals(createTableRequest2, sut.getCreateTableRequest(virtualTable2).get());
        // resolved by neither
        assertFalse(sut.getCreateTableRequest(virtualTable3).isPresent());
    }

    @Test
    void testIteratingCreateTableRequestFactory_precreateTables() {
        assertEquals(ImmutableList.of(createTableRequest1, createTableRequest2),
            new CreateTableRequestFactoryEnsemble(ImmutableList.of(ctrf1, ctrf2)).getPhysicalTables());
    }

}