package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.sharedtable.CreateTableRequestFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * Factory that contains a single create request and returns it for all virtual tables.
 */
public class SingletonCreateTableRequestFactory implements CreateTableRequestFactory {

    private final CreateTableRequest createTableRequest;

    protected SingletonCreateTableRequestFactory(CreateTableRequest createTableRequest) {
        this.createTableRequest = createTableRequest;
    }

    @Override
    public Optional<CreateTableRequest> getCreateTableRequest(DynamoTableDescription virtualTableDescription) {
        return Optional.of(createTableRequest);
    }

    @Override
    public List<CreateTableRequest> getPhysicalTables() {
        return Collections.singletonList(createTableRequest);
    }
}
