package com.salesforce.dynamodbv2.mt.mappers.sharedtable.impl;

import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.B;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.salesforce.dynamodbv2.mt.context.MtAmazonDynamoDbContextProvider;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

class BinaryFieldMapper implements FieldMapper {

    private static final FieldPrefixFunction<ByteBuffer> fieldPrefixFunction = new BinaryFieldPrefixFunction();

    private final MtAmazonDynamoDbContextProvider mtContext;
    private final String virtualTableName;

    BinaryFieldMapper(MtAmazonDynamoDbContextProvider mtContext,
                      String virtualTableName) {
        this.mtContext = mtContext;
        this.virtualTableName = virtualTableName;
    }

    @Override
    public AttributeValue apply(FieldMapping fieldMapping, AttributeValue unqualifiedAttribute) {
        checkArgument(fieldMapping.getTarget().getType() == B);
        ByteBuffer binaryValue = convertToBinary(fieldMapping.getSource().getType(), unqualifiedAttribute);
        FieldValue<ByteBuffer> fieldValue = new FieldValue<>(mtContext.getContext(), virtualTableName, binaryValue);
        return new AttributeValue().withB(fieldPrefixFunction.apply(fieldValue));
    }

    @Override
    public AttributeValue reverse(FieldMapping fieldMapping, AttributeValue qualifiedAttribute) {
        checkArgument(fieldMapping.getSource().getType() == B);
        FieldValue<ByteBuffer> fieldValue = fieldPrefixFunction.reverse(qualifiedAttribute.getB());
        return convertFromBinary(fieldMapping.getTarget().getType(), fieldValue.getValue());
    }

    private ByteBuffer convertToBinary(ScalarAttributeType type, AttributeValue attributeValue) {
        checkNotNull(type, "null attribute type");
        switch (type) {
            case S:
                return ByteBuffer.wrap(attributeValue.getS().getBytes(UTF_8));
            case N:
                BigDecimal bigDecimal = new BigDecimal(attributeValue.getN());
                int scale = bigDecimal.scale();
                byte[] unscaled = bigDecimal.unscaledValue().toByteArray();
                return ByteBuffer.allocate(4 + unscaled.length).putInt(scale).put(unscaled).flip();
            case B:
                return attributeValue.getB();
            default:
                throw new IllegalArgumentException("unexpected type " + type + " encountered");
        }
    }

    private AttributeValue convertFromBinary(ScalarAttributeType type, ByteBuffer value) {
        AttributeValue unqualifiedAttribute = new AttributeValue();
        switch (type) {
            case S:
                return unqualifiedAttribute.withS(new String(value.array(), UTF_8));
            case N:
                int scale = value.getInt();
                BigInteger unscaled = new BigInteger(value.array(), 4, value.remaining());
                return new AttributeValue().withN(new BigDecimal(unscaled, scale).toPlainString());
            case B:
                return unqualifiedAttribute.withB(value);
            default:
                throw new IllegalArgumentException("unexpected type " + type + " encountered");
        }
    }

}
