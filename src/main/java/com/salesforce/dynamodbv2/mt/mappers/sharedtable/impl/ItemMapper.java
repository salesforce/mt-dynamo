package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Maps items representing records in virtual tables so they can be read from and written to their physical table
 * counterparts.
 */
interface IItemMapper {

    /**
     * Takes a virtual table record (not qualified with multitenant context) to be upserted and converts it into the
     * corresponding physical table record (qualified with multitenant context). Used for adding context to
     * put/update requests.
     */
    Map<String, AttributeValue> applyForWrite(Map<String, AttributeValue> virtualItem);

    /**
     * Takes a virtual table record (not qualified with multitenant context) and returns the corresponding physical
     * table record with the table primary key attribute(s) only, or if a virtual secondary index is specified, with the
     * table table primary key attribute(s) and the secondary index attribute(s) only.
     */
    Map<String, AttributeValue> applyToKeyAttributes(Map<String, AttributeValue> virtualItem,
                                                     @Nullable DynamoSecondaryIndex virtualSecondaryIndex);

    /**
     * Takes a physical table record (qualified with multitenant context) and converts it into the corresponding virtual
     * table record (with qualifications removed). Used to convert get/query/scan results.
     */
    Map<String, AttributeValue> reverse(Map<String, AttributeValue> physicalItem);

}
