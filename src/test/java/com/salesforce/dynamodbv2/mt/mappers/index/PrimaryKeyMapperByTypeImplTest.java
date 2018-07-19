/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.dynamodbv2.mt.mappers.index;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.mappers.MappingException;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.N;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.S;
import static com.google.common.collect.ImmutableList.of;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 * @author msgroi
 */
class PrimaryKeyMapperByTypeImplTest {

    private static final HasPrimaryKey s = primaryKeyWrapper(S);
    private static final HasPrimaryKey n = primaryKeyWrapper(N);
    private static final HasPrimaryKey b = primaryKeyWrapper(B);
    private static final HasPrimaryKey ss = primaryKeyWrapper(S, S);
    private static final HasPrimaryKey sn = primaryKeyWrapper(S, N);
    private static final HasPrimaryKey sb = primaryKeyWrapper(S, B);

    private static final PrimaryKeyMapperByTypeImpl sut = new PrimaryKeyMapperByTypeImpl(false);
    private static final PrimaryKeyMapperByTypeImpl sutStrict = new PrimaryKeyMapperByTypeImpl(true);

    @Test
    void testS() throws MappingException {
        assertEquals(s, sut.mapPrimaryKey(s.getPrimaryKey(), of(s, b, n, ss, sn, sb)));
        assertEquals(ss, sut.mapPrimaryKey(s.getPrimaryKey(), of(b, n, ss, sn, sb)));
        assertEquals(sn, sut.mapPrimaryKey(s.getPrimaryKey(), of(b, n, sn, sb)));
        assertEquals(sb, sut.mapPrimaryKey(s.getPrimaryKey(), of(b, n, sb)));
        assertMappingException(() -> sut.mapPrimaryKey(s.getPrimaryKey(), of(n, b)));
        assertMappingException(() -> sut.mapPrimaryKey(s.getPrimaryKey(), of(s, s)));
        assertMappingException(() -> sut.mapPrimaryKey(ss.getPrimaryKey(), of(ss, ss)));
        assertEquals(s, sutStrict.mapPrimaryKey(s.getPrimaryKey(), of(s, b, n, ss, sn, sb)));
        assertMappingException(() -> sutStrict.mapPrimaryKey(s.getPrimaryKey(), of(b, n, ss, sn, sb)));
        assertMappingException(() -> sutStrict.mapPrimaryKey(s.getPrimaryKey(), of(b, n, sn, sb)));
        assertMappingException(() -> sutStrict.mapPrimaryKey(s.getPrimaryKey(), of(b, n, sb)));
    }

    @SuppressWarnings("Duplicates")
    @Test
    void testN() throws MappingException {
        assertEquals(s, sut.mapPrimaryKey(n.getPrimaryKey(), of(s, b, n, ss, sn, sb)));
        assertEquals(s, sut.mapPrimaryKey(n.getPrimaryKey(), of(s, b, ss, sn, sb)));
        assertEquals(ss, sut.mapPrimaryKey(n.getPrimaryKey(), of(b, ss, sn, sb)));
        assertMappingException(() -> sut.mapPrimaryKey(n.getPrimaryKey(), of(n, b)));
        assertMappingException(() -> sut.mapPrimaryKey(n.getPrimaryKey(), of(s, s)));
        assertEquals(s, sutStrict.mapPrimaryKey(n.getPrimaryKey(), of(s, b, n, ss, sn, sb)));
        assertMappingException(() -> sutStrict.mapPrimaryKey(n.getPrimaryKey(), of(b, ss, sn, sb)));
    }

    @SuppressWarnings("Duplicates")
    @Test
    void testB() throws MappingException {
        assertEquals(s, sut.mapPrimaryKey(b.getPrimaryKey(), of(s, b, n, ss, sn, sb)));
        assertEquals(s, sut.mapPrimaryKey(b.getPrimaryKey(), of(s, n, ss, sn, sb)));
        assertEquals(ss, sut.mapPrimaryKey(b.getPrimaryKey(), of(n, ss, sn, sb)));
        assertMappingException(() -> sut.mapPrimaryKey(n.getPrimaryKey(), of(n, b)));
        assertMappingException(() -> sut.mapPrimaryKey(b.getPrimaryKey(), of(s, s)));
        assertEquals(s, sutStrict.mapPrimaryKey(b.getPrimaryKey(), of(s, b, n, ss, sn, sb)));
        assertMappingException(() -> sutStrict.mapPrimaryKey(b.getPrimaryKey(), of(n, ss, sn, sb)));
    }

    @Test
    void testSs() throws MappingException {
        assertEquals(ss, sut.mapPrimaryKey(ss.getPrimaryKey(), of(s, b, n, ss, sn, sb)));
        assertMappingException(() -> sut.mapPrimaryKey(ss.getPrimaryKey(), of(s, b, n, sn, sb)));
        assertMappingException(() -> sut.mapPrimaryKey(ss.getPrimaryKey(), of(ss, ss)));
    }

    @Test
    void testSn() throws MappingException {
        assertEquals(sn, sut.mapPrimaryKey(sn.getPrimaryKey(), of(s, b, n, ss, sn, sb)));
        assertMappingException(() -> sut.mapPrimaryKey(sn.getPrimaryKey(), of(s, b, n, ss, sb)));
        assertMappingException(() -> sut.mapPrimaryKey(sn.getPrimaryKey(), of(ss, ss)));
    }

    @Test
    void testSb() throws MappingException {
        assertEquals(sb, sut.mapPrimaryKey(sb.getPrimaryKey(), of(s, b, n, ss, sn, sb)));
        assertMappingException(() -> sut.mapPrimaryKey(sb.getPrimaryKey(), of(s, b, n, ss, sn)));
        assertMappingException(() -> sut.mapPrimaryKey(sb.getPrimaryKey(), of(ss, ss)));
    }

    private static void assertMappingException(TestFunction test) {
        try {
            HasPrimaryKey hasPrimaryKey = test.run();
            throw new RuntimeException("expected MappingException not encountered, found:" + hasPrimaryKey.getPrimaryKey());
        } catch (MappingException ignored) { /* expected */ }
    }

    @FunctionalInterface
    private interface TestFunction {
        HasPrimaryKey run() throws MappingException;
    }

    private static HasPrimaryKey primaryKeyWrapper(ScalarAttributeType hashKeyType) {
        return () -> new PrimaryKey("", hashKeyType, Optional.empty(), Optional.empty());
    }

    @SuppressWarnings("all")
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