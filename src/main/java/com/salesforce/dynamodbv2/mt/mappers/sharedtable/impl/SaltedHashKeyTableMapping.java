package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.amazonaws.services.dynamodbv2.local.shared.access.LocalDBUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import com.salesforce.dynamodbv2.mt.mappers.index.DynamoSecondaryIndex;
import com.salesforce.dynamodbv2.mt.mappers.metadata.DynamoTableDescription;
import com.salesforce.dynamodbv2.mt.mappers.metadata.PrimaryKey;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SaltedHashKeyTableMapping implements TableMapping {

    private static final int MAX_KEY_LENGTH = 1024;

    private final int numSaltBuckets;
    private final char delimiter;
    private final MtAmazonDynamoDbContextProvider mtContext;
    private final DynamoTableDescription virtualTable;
    private final DynamoTableDescription physicalTable;

    public SaltedHashKeyTableMapping(MtAmazonDynamoDbContextProvider mtContext,
        DynamoTableDescription physicalTable,
        DynamoTableDescription virtualTable,
        byte numSaltBuckets,
        char delimiter) {
        this.mtContext = mtContext;
        this.virtualTable = virtualTable;
        this.physicalTable = physicalTable;
        this.numSaltBuckets = numSaltBuckets;
        this.delimiter = delimiter;
    }

    @Override
    public DynamoTableDescription getVirtualTable() {
        return virtualTable;
    }

    @Override
    public DynamoTableDescription getPhysicalTable() {
        return physicalTable;
    }

    @Override
    public ItemMapper getItemMapper() {
        return new ItemMapper() {
            @Override
            public Map<String, AttributeValue> apply(Map<String, AttributeValue> virtualItem) {
                // TODO BB consider modifying item in place to reduce cost
                Map<String, AttributeValue> item = new HashMap<>(virtualItem);

                // TODO BB validate at higher level that no more vGSIs than pGSIs
                // first copy index fields
                Iterator<DynamoSecondaryIndex> vIt = virtualTable.getGsis().iterator();
                Iterator<DynamoSecondaryIndex> pIt = physicalTable.getGsis().iterator();
                while (vIt.hasNext()) {
                    addPhysicalKeys(item, vIt.next().getPrimaryKey(), pIt.next().getPrimaryKey(), false);
                }

                // then move primary key fields
                PrimaryKey virtualPK = virtualTable.getPrimaryKey();
                PrimaryKey physicalPK = physicalTable.getPrimaryKey();
                addPhysicalKeys(item, virtualPK, physicalPK, true);

                return item;
            }

            private void addPhysicalKeys(Map<String, AttributeValue> item, PrimaryKey virtualPK,
                PrimaryKey physicalPK, boolean required) {
                String hk = virtualPK.getHashKey();
                AttributeValue hashKey = item.get(hk);
                virtualPK.getRangeKey().ifPresentOrElse(rk -> {
                    AttributeValue rangeKey = item.get(rk);
                    if (hashKey != null && rangeKey != null) {
                        // TODO BB normalize to primitive object before hashing
                        int salt = Objects.hashCode(hashKey) % numSaltBuckets;
                        // TODO BB this is tied to the physical table schema (should be kept together)!
                        item.put(physicalPK.getHashKey(),
                            new AttributeValue(
                                mtContext.getContext() + delimiter + virtualTable.getTableName() + delimiter
                                    + salt));
                        item.put(physicalPK.getRangeKey().get(),
                            new AttributeValue()
                                .withB(toBytes(virtualPK.getHashKeyType(), hashKey, virtualPK.getRangeKeyType().get(),
                                    rangeKey)));
                        if (required) {
                            item.remove(hk);
                            item.remove(rk);
                        }
                    } else {
                        if (required) {
                            throw new RuntimeException(
                                "Item is missing " + (hashKey == null
                                    ? (rangeKey == null
                                    ? "hash key '" + hk + "' and range key '" + rk + "'"
                                    : "hash key '" + hk + "'")
                                    : "range key '" + rk + "'"));
                        }
                    }
                }, () -> {
                    if (hashKey != null) {
                        // TODO normalize to primitive object before hashing
                        int salt = Objects.hashCode(hashKey) % numSaltBuckets;
                        item.put(physicalPK.getHashKey(),
                            new AttributeValue(
                                mtContext.getContext() + delimiter + virtualTable.getTableName() + delimiter
                                    + salt));
                        item.put(physicalPK.getRangeKey().get(),
                            new AttributeValue()
                                .withB(toBytes(virtualPK.getHashKeyType(), hashKey)));
                        if (required) {
                            item.remove(hk);
                        }
                    } else {
                        if (required) {
                            throw new RuntimeException("Item is missing hash key '" + hk + "'");
                        }
                    }
                });
            }

            private ByteBuffer toBytes(ScalarAttributeType hkt, AttributeValue hk, ScalarAttributeType rkt,
                AttributeValue rk) {
                byte[] hkb = toByteArray(hkt, hk);
                byte[] rkb = toByteArray(rkt, rk);
                Preconditions.checkArgument(hkb.length + rkb.length <= MAX_KEY_LENGTH - 2);
                ByteBuffer key = ByteBuffer.allocate(2 + hkb.length + rkb.length);
                return key.putShort((short) hkb.length).put(hkb).put(rkb).flip();
            }

            private ByteBuffer toBytes(ScalarAttributeType type, AttributeValue value) {
                return ByteBuffer.wrap(toByteArray(type, value));
            }

            private byte[] toByteArray(ScalarAttributeType type, AttributeValue value) {
                switch (type) {
                    case S:
                        return value.getS().getBytes(UTF_8);
                    case N:
                        return LocalDBUtils.encodeBigDecimal(new BigDecimal(value.getN()));
                    case B:
                        return value.getB().array();
                }
                throw new RuntimeException("Unhandled case");
            }

            @Override
            public Map<String, AttributeValue> reverse(Map<String, AttributeValue> physicalItem) {
                // TODO BB consider modifying item in place to reduce cost
                Map<String, AttributeValue> item = new HashMap<>(physicalItem);

                // TODO BB validate at higher level that no more vGSIs than pGSIs
                // first copy index fields
                Iterator<DynamoSecondaryIndex> vIt = virtualTable.getGsis().iterator();
                Iterator<DynamoSecondaryIndex> pIt = physicalTable.getGsis().iterator();
                while (vIt.hasNext()) {
                    removePhysicalKeys(item, vIt.next().getPrimaryKey(), pIt.next().getPrimaryKey(), false);
                }

                // then move primary key fields
                PrimaryKey virtualPK = virtualTable.getPrimaryKey();
                PrimaryKey physicalPK = physicalTable.getPrimaryKey();
                removePhysicalKeys(item, virtualPK, physicalPK, true);

                return item;
            }

            private void removePhysicalKeys(Map<String, AttributeValue> item, PrimaryKey virtualPK,
                PrimaryKey physicalPK, boolean required) {
                item.remove(physicalPK.getHashKey());
                AttributeValue value = item.remove(physicalPK.getRangeKey().get());
                if (required) {
                    virtualPK.getRangeKey().ifPresentOrElse(rk -> {
                        AttributeValue[] vs = fromBytes(virtualPK.getHashKeyType(), virtualPK.getRangeKeyType().get(),
                            value.getB());
                        item.put(virtualPK.getHashKey(), vs[0]);
                        item.put(rk, vs[1]);
                    }, () -> {
                        item.put(virtualPK.getHashKey(), fromBytes(virtualPK.getHashKeyType(), value.getB()));
                    });
                }
            }

            private AttributeValue[] fromBytes(ScalarAttributeType hkt, ScalarAttributeType rkt, ByteBuffer buf) {
                short s = buf.getShort();
                AttributeValue hkv = fromBytes(hkt, buf, s);
                AttributeValue rkv = fromBytes(rkt, buf, buf.remaining());
                return new AttributeValue[]{hkv, rkv};
            }

            private AttributeValue fromBytes(ScalarAttributeType type, ByteBuffer buf) {
                return fromBytes(type, buf, buf.limit());
            }

            private AttributeValue fromBytes(ScalarAttributeType type, ByteBuffer buf, int size) {
                switch (type) {
                    case S: {
                        byte[] bytes = new byte[size];
                        buf.get(bytes);
                        return new AttributeValue(new String(bytes, UTF_8));
                    }
                    case N: {
                        byte[] bytes = new byte[size];
                        buf.get(bytes);
                        return new AttributeValue().withN(LocalDBUtils.decodeBigDecimal(bytes).toString());
                    }
                    case B: {
                        int limit = buf.position() + size;
                        ByteBuffer dup = buf.duplicate().position(buf.position()).limit(limit);
                        buf.position(limit);
                        return new AttributeValue().withB(dup);
                    }
                }
                throw new RuntimeException("Unhandled case");
            }

        };
    }

    @Override
    public QueryAndScanMapper getQueryAndScanMapper() {
        return null;
    }

    @Override
    public ConditionMapper getConditionMapper() {
        return new ConditionMapper() {
            @Override
            public void apply(RequestWrapper request) {

            }
        };
    }

}
