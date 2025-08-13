package com.facebook.presto.sql.analyzer;

import com.google.protobuf.Any;
import io.substrait.relation.Extension;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;

import java.util.Objects;

/**
 * Partial Copy of https://github.com/substrait-io/substrait-java/blob/18a12d5b8c14ee3101c34af2211104e462e9ffb2/core/src/test/java/io/substrait/relation/utils/StringHolder.java
 */
public class SubstraitSingleRelExtensionDetail
        implements Extension.SingleRelDetail
{

    private final String value;

    public SubstraitSingleRelExtensionDetail(String value)
    {
        this.value = value;
    }

    @Override
    public Any toProto(RelProtoConverter relProtoConverter)
    {
        return com.google.protobuf.Any.pack(com.google.protobuf.StringValue.of(this.value));
    }

    @Override
    public Type.Struct deriveRecordType(Rel input)
    {
        return Type.Struct.builder()
                .nullable(false)
                .addAllFields(input.getRecordType().fields())
                .build();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubstraitSingleRelExtensionDetail that = (SubstraitSingleRelExtensionDetail) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public String toString()
    {
        return value;
    }
}
