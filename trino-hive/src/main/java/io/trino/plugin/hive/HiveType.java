/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveStorageFormat.AVRO;
import static io.prestosql.plugin.hive.HiveStorageFormat.ORC;
import static io.prestosql.plugin.hive.util.HiveTypeTranslator.fromPrimitiveType;
import static io.prestosql.plugin.hive.util.HiveTypeTranslator.toTypeInfo;
import static io.prestosql.plugin.hive.util.HiveTypeTranslator.toTypeSignature;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.binaryTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.dateTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.timestampTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfosFromTypeString;

public final class HiveType
{
    public static final HiveType HIVE_BOOLEAN = new HiveType(booleanTypeInfo);
    public static final HiveType HIVE_BYTE = new HiveType(byteTypeInfo);
    public static final HiveType HIVE_SHORT = new HiveType(shortTypeInfo);
    public static final HiveType HIVE_INT = new HiveType(intTypeInfo);
    public static final HiveType HIVE_LONG = new HiveType(longTypeInfo);
    public static final HiveType HIVE_FLOAT = new HiveType(floatTypeInfo);
    public static final HiveType HIVE_DOUBLE = new HiveType(doubleTypeInfo);
    public static final HiveType HIVE_STRING = new HiveType(stringTypeInfo);
    public static final HiveType HIVE_TIMESTAMP = new HiveType(timestampTypeInfo);
    public static final HiveType HIVE_DATE = new HiveType(dateTypeInfo);
    public static final HiveType HIVE_BINARY = new HiveType(binaryTypeInfo);

    private final HiveTypeName hiveTypeName;
    private final TypeInfo typeInfo;

    private HiveType(TypeInfo typeInfo)
    {
        requireNonNull(typeInfo, "typeInfo is null");
        this.hiveTypeName = new HiveTypeName(typeInfo.getTypeName());
        this.typeInfo = typeInfo;
    }

    public HiveTypeName getHiveTypeName()
    {
        return hiveTypeName;
    }

    public Category getCategory()
    {
        return typeInfo.getCategory();
    }

    public TypeInfo getTypeInfo()
    {
        return typeInfo;
    }

    public TypeSignature getTypeSignature()
    {
        return toTypeSignature(typeInfo);
    }

    @Deprecated
    public Type getType(TypeManager typeManager)
    {
        return typeManager.getType(getTypeSignature());
    }

    public Type getType(TypeManager typeManager, HiveTimestampPrecision timestampPrecision)
    {
        Type tentativeType = typeManager.getType(getTypeSignature());
        // TODO: handle timestamps in structural types (https://github.com/prestosql/presto/issues/5195)
        if (tentativeType instanceof TimestampType) {
            return TimestampType.createTimestampType(timestampPrecision.getPrecision());
        }
        return tentativeType;
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

        HiveType hiveType = (HiveType) o;

        return hiveTypeName.equals(hiveType.hiveTypeName);
    }

    @Override
    public int hashCode()
    {
        return hiveTypeName.hashCode();
    }

    @JsonValue
    @Override
    public String toString()
    {
        return hiveTypeName.toString();
    }

    public boolean isSupportedType(StorageFormat storageFormat)
    {
        return isSupportedType(getTypeInfo(), storageFormat);
    }

    public static boolean isSupportedType(TypeInfo typeInfo, StorageFormat storageFormat)
    {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                return fromPrimitiveType((PrimitiveTypeInfo) typeInfo) != null;
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return isSupportedType(mapTypeInfo.getMapKeyTypeInfo(), storageFormat) && isSupportedType(mapTypeInfo.getMapValueTypeInfo(), storageFormat);
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                return isSupportedType(listTypeInfo.getListElementTypeInfo(), storageFormat);
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                return structTypeInfo.getAllStructFieldTypeInfos().stream()
                        .allMatch(fieldTypeInfo -> isSupportedType(fieldTypeInfo, storageFormat));
            case UNION:
                // This feature (reading uniontypes as structs) has only been verified against Avro and ORC tables. Here's a discussion:
                //   1. Avro tables are supported and verified.
                //   2. ORC tables are supported and verified.
                //   3. The Parquet format doesn't support uniontypes itself so there's no need to add support for it in Presto.
                //   4. TODO: RCFile tables are not supported yet.
                //   5. TODO: The support for Avro is done in SerDeUtils so it's possible that formats other than Avro are also supported. But verification is needed.
                if (storageFormat.getSerDe().equalsIgnoreCase(AVRO.getSerDe()) || storageFormat.getSerDe().equalsIgnoreCase(ORC.getSerDe())) {
                    UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
                    return unionTypeInfo.getAllUnionObjectTypeInfos().stream()
                            .allMatch(fieldTypeInfo -> isSupportedType(fieldTypeInfo, storageFormat));
                }
        }
        return false;
    }

    @JsonCreator
    public static HiveType valueOf(String hiveTypeName)
    {
        requireNonNull(hiveTypeName, "hiveTypeName is null");
        return toHiveType(getTypeInfoFromTypeString(hiveTypeName));
    }

    public static List<HiveType> toHiveTypes(String hiveTypes)
    {
        requireNonNull(hiveTypes, "hiveTypes is null");
        return ImmutableList.copyOf(getTypeInfosFromTypeString(hiveTypes).stream()
                .map(HiveType::toHiveType)
                .collect(toImmutableList()));
    }

    public static HiveType toHiveType(TypeInfo typeInfo)
    {
        requireNonNull(typeInfo, "typeInfo is null");
        return new HiveType(typeInfo);
    }

    public static HiveType toHiveType(Type type)
    {
        return new HiveType(toTypeInfo(type));
    }

    public Optional<HiveType> getHiveTypeForDereferences(List<Integer> dereferences)
    {
        TypeInfo typeInfo = getTypeInfo();
        for (int fieldIndex : dereferences) {
            checkArgument(typeInfo instanceof StructTypeInfo, "typeInfo should be struct type", typeInfo);
            StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
            try {
                typeInfo = structTypeInfo.getAllStructFieldTypeInfos().get(fieldIndex);
            }
            catch (RuntimeException e) {
                return Optional.empty();
            }
        }
        return Optional.of(toHiveType(typeInfo));
    }

    public List<String> getHiveDereferenceNames(List<Integer> dereferences)
    {
        ImmutableList.Builder<String> dereferenceNames = ImmutableList.builder();
        TypeInfo typeInfo = getTypeInfo();
        for (int fieldIndex : dereferences) {
            checkArgument(typeInfo instanceof StructTypeInfo, "typeInfo should be struct type", typeInfo);
            StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;

            checkArgument(fieldIndex >= 0, "fieldIndex cannot be negative");
            checkArgument(fieldIndex < structTypeInfo.getAllStructFieldNames().size(),
                    "fieldIndex should be less than the number of fields in the struct");
            String fieldName = structTypeInfo.getAllStructFieldNames().get(fieldIndex);
            dereferenceNames.add(fieldName);
            typeInfo = structTypeInfo.getAllStructFieldTypeInfos().get(fieldIndex);
        }

        return dereferenceNames.build();
    }
}
