/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.collect.ImmutableList.of;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * TODO: write Javadoc.
 *
 * @author msgroi
 */
class PrimaryKeyMapperByTypeImplTest {

    private static final HasPrimaryKey HPK_S = primaryKeyWrapper(S);
    private static final HasPrimaryKey HPK_N = primaryKeyWrapper(N);
    private static final HasPrimaryKey HPK_B = primaryKeyWrapper(B);
    private static final HasPrimaryKey HPK_SS = primaryKeyWrapper(S, S);
    private static final HasPrimaryKey HPK_SN = primaryKeyWrapper(S, N);
    private static final HasPrimaryKey HPK_SB = primaryKeyWrapper(S, B);

    private static final PrimaryKeyMapperByTypeImpl SUT = new PrimaryKeyMapperByTypeImpl(false);
    private static final PrimaryKeyMapperByTypeImpl SUT_STRICT = new PrimaryKeyMapperByTypeImpl(true);

    @Test
    void testS() throws MappingException {
        assertEquals(HPK_S, SUT.mapPrimaryKey(HPK_S.getPrimaryKey(), of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertEquals(HPK_SS, SUT.mapPrimaryKey(HPK_S.getPrimaryKey(), of(HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertEquals(HPK_SN, SUT.mapPrimaryKey(HPK_S.getPrimaryKey(), of(HPK_B, HPK_N, HPK_SN, HPK_SB)));
        assertEquals(HPK_SB, SUT.mapPrimaryKey(HPK_S.getPrimaryKey(), of(HPK_B, HPK_N, HPK_SB)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_S.getPrimaryKey(), of(HPK_N, HPK_B)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_S.getPrimaryKey(), of(HPK_S, HPK_S)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_SS.getPrimaryKey(), of(HPK_SS, HPK_SS)));
        assertEquals(HPK_S,
                SUT_STRICT.mapPrimaryKey(HPK_S.getPrimaryKey(), of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT_STRICT.mapPrimaryKey(HPK_S.getPrimaryKey(),
                of(HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT_STRICT.mapPrimaryKey(HPK_S.getPrimaryKey(), of(HPK_B, HPK_N, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT_STRICT.mapPrimaryKey(HPK_S.getPrimaryKey(), of(HPK_B, HPK_N, HPK_SB)));
    }

    @Test
    void testN() throws MappingException {
        assertEquals(HPK_S, SUT.mapPrimaryKey(HPK_N.getPrimaryKey(), of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertEquals(HPK_S, SUT.mapPrimaryKey(HPK_N.getPrimaryKey(), of(HPK_S, HPK_B, HPK_SS, HPK_SN, HPK_SB)));
        assertEquals(HPK_SS, SUT.mapPrimaryKey(HPK_N.getPrimaryKey(), of(HPK_B, HPK_SS, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_N.getPrimaryKey(), of(HPK_N, HPK_B)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_N.getPrimaryKey(), of(HPK_S, HPK_S)));
        assertEquals(HPK_S,
                SUT_STRICT.mapPrimaryKey(HPK_N.getPrimaryKey(), of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT_STRICT.mapPrimaryKey(HPK_N.getPrimaryKey(),
                of(HPK_B, HPK_SS, HPK_SN, HPK_SB)));
    }

    @Test
    void testB() throws MappingException {
        assertEquals(HPK_S, SUT.mapPrimaryKey(HPK_B.getPrimaryKey(), of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertEquals(HPK_S, SUT.mapPrimaryKey(HPK_B.getPrimaryKey(), of(HPK_S, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertEquals(HPK_SS, SUT.mapPrimaryKey(HPK_B.getPrimaryKey(), of(HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_N.getPrimaryKey(), of(HPK_N, HPK_B)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_B.getPrimaryKey(), of(HPK_S, HPK_S)));
        assertEquals(HPK_S,
                SUT_STRICT.mapPrimaryKey(HPK_B.getPrimaryKey(), of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT_STRICT.mapPrimaryKey(HPK_B.getPrimaryKey(),
                of(HPK_N, HPK_SS, HPK_SN, HPK_SB)));
    }

    @Test
    void testSs() throws MappingException {
        assertEquals(HPK_SS,
                SUT.mapPrimaryKey(HPK_SS.getPrimaryKey(), of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_SS.getPrimaryKey(),
                of(HPK_S, HPK_B, HPK_N, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_SS.getPrimaryKey(), of(HPK_SS, HPK_SS)));
    }

    @Test
    void testSn() throws MappingException {
        assertEquals(HPK_SN,
                SUT.mapPrimaryKey(HPK_SN.getPrimaryKey(), of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_SN.getPrimaryKey(),
                of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SB)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_SN.getPrimaryKey(), of(HPK_SS, HPK_SS)));
    }

    @Test
    void testSb() throws MappingException {
        assertEquals(HPK_SB,
                SUT.mapPrimaryKey(HPK_SB.getPrimaryKey(), of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SN, HPK_SB)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_SB.getPrimaryKey(),
                of(HPK_S, HPK_B, HPK_N, HPK_SS, HPK_SN)));
        assertMappingException(() -> SUT.mapPrimaryKey(HPK_SB.getPrimaryKey(), of(HPK_SS, HPK_SS)));
    }

    private static void assertMappingException(TestFunction test) {
        try {
            HasPrimaryKey hasPrimaryKey = test.run();
            throw new RuntimeException("expected MappingException not encountered, found:"
                    + hasPrimaryKey.getPrimaryKey());
        } catch (MappingException ignored) { /* expected */ }
    }

    @FunctionalInterface
    private interface TestFunction {
        HasPrimaryKey run() throws MappingException;
    }

    private static HasPrimaryKey primaryKeyWrapper(ScalarAttributeType hashKeyType) {
        return () -> new PrimaryKey("", hashKeyType, Optional.empty(), Optional.empty());
    }

    private static HasPrimaryKey primaryKeyWrapper(ScalarAttributeType hashKeyType, ScalarAttributeType rangeKeyType) {
        return () -> new PrimaryKey("", hashKeyType, Optional.of(""), Optional.of(rangeKeyType));
    }

    private void assertEquals(HasPrimaryKey hasPrimaryKey1, HasPrimaryKey hasPrimaryKey2) {
        PrimaryKey pk1 = hasPrimaryKey1.getPrimaryKey();
        PrimaryKey pk2 = hasPrimaryKey2.getPrimaryKey();
        org.junit.jupiter.api.Assertions.assertEquals(pk1.getHashKeyType(), pk2.getHashKeyType());
        assertTrue((pk1.getRangeKey().isPresent() && pk2.getRangeKey().isPresent())
                || (!pk1.getRangeKey().isPresent() && !pk2.getRangeKey().isPresent()),
            () -> "expected range key to be " + (pk1.getRangeKeyType().isPresent()
                ? "present with type=" + pk1.getRangeKeyType().get()
                : "NOT present but found with type=" + pk2.getRangeKeyType().get()));
        if (pk1.getRangeKeyType().isPresent()) {
            org.junit.jupiter.api.Assertions.assertEquals(pk1.getRangeKeyType().get(), pk2.getRangeKeyType().get());
        }
    }

}