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
package io.trino.parquet.spark;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Copied from https://github.com/apache/spark/blob/53d65fd12dd9231139188227ef9040d40d759021/common/variant/src/main/java/org/apache/spark/types/variant/VariantUtil.java
 *
 * This class defines constants related to the variant format and provides functions for
 * manipulating variant binaries.

 * A variant is made up of 2 binaries: value and metadata. A variant value consists of a one-byte
 * header and a number of content bytes (can be zero). The header byte is divided into upper 6 bits
 * (called "type info") and lower 2 bits (called "basic type"). The content format is explained in
 * the below constants for all possible basic type and type info values.

 * The variant metadata includes a version id and a dictionary of distinct strings (case-sensitive).
 * Its binary format is:
 * - Version: 1-byte unsigned integer. The only acceptable value is 1 currently.
 * - Dictionary size: 4-byte little-endian unsigned integer. The number of keys in the
 * dictionary.
 * - Offsets: (size + 1) * 4-byte little-endian unsigned integers. `offsets[i]` represents the
 * starting position of string i, counting starting from the address of `offsets[0]`. Strings
 * must be stored contiguously, so we don’t need to store the string size, instead, we compute it
 * with `offset[i + 1] - offset[i]`.
 * - UTF-8 string data.
 */
public final class VariantUtil
{
    public static final int BASIC_TYPE_BITS = 2;
    public static final int BASIC_TYPE_MASK = 0x3;
    public static final int TYPE_INFO_MASK = 0x3F;

    // Below is all possible basic type values.
    // Primitive value. The type info value must be one of the values in the below section.
    public static final int PRIMITIVE = 0;
    // Short string value. The type info value is the string size, which must be in `[0,
    // kMaxShortStrSize]`.
    // The string content bytes directly follow the header byte.
    public static final int SHORT_STR = 1;
    // Object value. The content contains a size, a list of field ids, a list of field offsets, and
    // the actual field data. The length of the id list is `size`, while the length of the offset
    // list is `size + 1`, where the last offset represent the total size of the field data. The
    // fields in an object must be sorted by the field name in alphabetical order. Duplicate field
    // names in one object are not allowed.
    // We use 5 bits in the type info to specify the integer type of the object header: it should
    // be 0_b4_b3b2_b1b0 (MSB is 0), where:
    // - b4 specifies the type of size. When it is 0/1, `size` is a little-endian 1/4-byte
    // unsigned integer.
    // - b3b2/b1b0 specifies the integer type of id and offset. When the 2 bits are  0/1/2, the
    // list contains 1/2/3-byte little-endian unsigned integers.
    public static final int OBJECT = 2;
    // Array value. The content contains a size, a list of field offsets, and the actual element
    // data. It is similar to an object without the id list. The length of the offset list
    // is `size + 1`, where the last offset represent the total size of the element data.
    // Its type info should be: 000_b2_b1b0:
    // - b2 specifies the type of size.
    // - b1b0 specifies the integer type of offset.
    public static final int ARRAY = 3;

    // Below is all possible type info values for `PRIMITIVE`.
    // JSON Null value. Empty content.
    public static final int NULL = 0;
    // True value. Empty content.
    public static final int TRUE = 1;
    // False value. Empty content.
    public static final int FALSE = 2;
    // 1-byte little-endian signed integer.
    public static final int INT1 = 3;
    // 2-byte little-endian signed integer.
    public static final int INT2 = 4;
    // 4-byte little-endian signed integer.
    public static final int INT4 = 5;
    // 4-byte little-endian signed integer.
    public static final int INT8 = 6;
    // 8-byte IEEE double.
    public static final int DOUBLE = 7;
    // 4-byte decimal. Content is 1-byte scale + 4-byte little-endian signed integer.
    public static final int DECIMAL4 = 8;
    // 8-byte decimal. Content is 1-byte scale + 8-byte little-endian signed integer.
    public static final int DECIMAL8 = 9;
    // 16-byte decimal. Content is 1-byte scale + 16-byte little-endian signed integer.
    public static final int DECIMAL16 = 10;
    // Date value. Content is 4-byte little-endian signed integer that represents the number of days
    // from the Unix epoch.
    public static final int DATE = 11;
    // Timestamp value. Content is 8-byte little-endian signed integer that represents the number of
    // microseconds elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC. It is displayed to users in
    // their local time zones and may be displayed differently depending on the execution environment.
    public static final int TIMESTAMP = 12;
    // Timestamp_ntz value. It has the same content as `TIMESTAMP` but should always be interpreted
    // as if the local time zone is UTC.
    public static final int TIMESTAMP_NTZ = 13;
    // 4-byte IEEE float.
    public static final int FLOAT = 14;
    // Binary value. The content is (4-byte little-endian unsigned integer representing the binary
    // size) + (size bytes of binary content).
    public static final int BINARY = 15;
    // Long string value. The content is (4-byte little-endian unsigned integer representing the
    // string size) + (size bytes of string content).
    public static final int LONG_STR = 16;

    public static final byte VERSION = 1;
    // The lower 4 bits of the first metadata byte contain the version.
    public static final byte VERSION_MASK = 0x0F;

    public static final int U24_MAX = 0xFFFFFF;
    public static final int U32_SIZE = 4;

    // Both variant value and variant metadata need to be no longer than 16MiB.
    public static final int SIZE_LIMIT = U24_MAX + 1;

    public static final int MAX_DECIMAL4_PRECISION = 9;
    public static final int MAX_DECIMAL8_PRECISION = 18;
    public static final int MAX_DECIMAL16_PRECISION = 38;

    private VariantUtil() {}

    // Check the validity of an array index `position`. Throw `MALFORMED_VARIANT` if it is out of bound,
    // meaning that the variant is malformed.
    static void checkIndex(int position, int length)
    {
        if (position < 0 || position >= length) {
            throw new IllegalArgumentException("Index out of bound: %s (length: %s)".formatted(position, length));
        }
    }

    // Read a little-endian signed long value from `bytes[position, position + numBytes)`.
    static long readLong(byte[] bytes, int position, int numBytes)
    {
        checkIndex(position, bytes.length);
        checkIndex(position + numBytes - 1, bytes.length);
        long result = 0;
        // All bytes except the most significant byte should be unsign-extended and shifted (so we need
        // `& 0xFF`). The most significant byte should be sign-extended and is handled after the loop.
        for (int i = 0; i < numBytes - 1; ++i) {
            long unsignedByteValue = bytes[position + i] & 0xFF;
            result |= unsignedByteValue << (8 * i);
        }
        long signedByteValue = bytes[position + numBytes - 1];
        result |= signedByteValue << (8 * (numBytes - 1));
        return result;
    }

    // Read a little-endian unsigned int value from `bytes[position, position + numBytes)`. The value must fit
    // into a non-negative int (`[0, Integer.MAX_VALUE]`).
    static int readUnsigned(byte[] bytes, int position, int numBytes)
    {
        checkIndex(position, bytes.length);
        checkIndex(position + numBytes - 1, bytes.length);
        int result = 0;
        // Similar to the `readLong` loop, but all bytes should be unsign-extended.
        for (int i = 0; i < numBytes; ++i) {
            int unsignedByteValue = bytes[position + i] & 0xFF;
            result |= unsignedByteValue << (8 * i);
        }
        if (result < 0) {
            throw new IllegalArgumentException("Value out of bound: %s".formatted(result));
        }
        return result;
    }

    // The value type of variant value. It is determined by the header byte but not a 1:1 mapping
    // (for example, INT1/2/4/8 all maps to `Type.LONG`).
    public enum Type
    {
        NULL,
        BOOLEAN,
        LONG,
        FLOAT,
        DOUBLE,
        DECIMAL,
        STRING,
        BINARY,
        DATE,
        TIMESTAMP,
        TIMESTAMP_NTZ,
        ARRAY,
        OBJECT,
    }

    // Get the value type of variant value `value[position...]`. It is only legal to call `get*` if
    // `getType` returns this type (for example, it is only legal to call `getLong` if `getType`
    // returns `Type.Long`).
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static Type getType(byte[] value, int position)
    {
        checkIndex(position, value.length);
        int basicType = value[position] & BASIC_TYPE_MASK;
        int typeInfo = (value[position] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        return switch (basicType) {
            case SHORT_STR -> Type.STRING;
            case OBJECT -> Type.OBJECT;
            case ARRAY -> Type.ARRAY;
            default -> switch (typeInfo) {
                case NULL -> Type.NULL;
                case TRUE, FALSE -> Type.BOOLEAN;
                case INT1, INT2, INT4, INT8 -> Type.LONG;
                case DOUBLE -> Type.DOUBLE;
                case DECIMAL4, DECIMAL8, DECIMAL16 -> Type.DECIMAL;
                case DATE -> Type.DATE;
                case TIMESTAMP -> Type.TIMESTAMP;
                case TIMESTAMP_NTZ -> Type.TIMESTAMP_NTZ;
                case FLOAT -> Type.FLOAT;
                case BINARY -> Type.BINARY;
                case LONG_STR -> Type.STRING;
                default -> throw new IllegalArgumentException("Unexpected type: " + typeInfo);
            };
        };
    }

    private static IllegalStateException unexpectedType(Type type)
    {
        return new IllegalStateException("Expect type to be " + type);
    }

    // Get a boolean value from variant value `value[position...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static boolean getBoolean(byte[] value, int position)
    {
        checkIndex(position, value.length);
        int basicType = value[position] & BASIC_TYPE_MASK;
        int typeInfo = (value[position] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE || (typeInfo != TRUE && typeInfo != FALSE)) {
            throw unexpectedType(Type.BOOLEAN);
        }
        return typeInfo == TRUE;
    }

    // Get a long value from variant value `value[position...]`.
    // It is only legal to call it if `getType` returns one of `Type.LONG/DATE/TIMESTAMP/
    // TIMESTAMP_NTZ`. If the type is `DATE`, the return value is guaranteed to fit into an int and
    // represents the number of days from the Unix epoch. If the type is `TIMESTAMP/TIMESTAMP_NTZ`,
    // the return value represents the number of microseconds from the Unix epoch.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static long getLong(byte[] value, int position)
    {
        checkIndex(position, value.length);
        int basicType = value[position] & BASIC_TYPE_MASK;
        int typeInfo = (value[position] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        String exceptionMessage = "Expect type to be LONG/DATE/TIMESTAMP/TIMESTAMP_NTZ";
        if (basicType != PRIMITIVE) {
            throw new IllegalStateException(exceptionMessage);
        }
        return switch (typeInfo) {
            case INT1 -> readLong(value, position + 1, 1);
            case INT2 -> readLong(value, position + 1, 2);
            case INT4, DATE -> readLong(value, position + 1, 4);
            case INT8, TIMESTAMP, TIMESTAMP_NTZ -> readLong(value, position + 1, 8);
            default -> throw new IllegalStateException(exceptionMessage);
        };
    }

    // Get a double value from variant value `value[position...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static double getDouble(byte[] value, int position)
    {
        checkIndex(position, value.length);
        int basicType = value[position] & BASIC_TYPE_MASK;
        int typeInfo = (value[position] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE || typeInfo != DOUBLE) {
            throw unexpectedType(Type.DOUBLE);
        }
        return Double.longBitsToDouble(readLong(value, position + 1, 8));
    }

    // Check whether the precision and scale of the decimal are within the limit.
    private static void checkDecimal(BigDecimal decimal, int maxPrecision)
    {
        if (decimal.precision() > maxPrecision || decimal.scale() > maxPrecision) {
            throw new IllegalArgumentException("Decimal out of bound: " + decimal);
        }
    }

    // Get a decimal value from variant value `value[position...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static BigDecimal getDecimal(byte[] value, int position)
    {
        checkIndex(position, value.length);
        int basicType = value[position] & BASIC_TYPE_MASK;
        int typeInfo = (value[position] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE) {
            throw unexpectedType(Type.DECIMAL);
        }
        // Interpret the scale byte as unsigned. If it is a negative byte, the unsigned value must be
        // greater than `MAX_DECIMAL16_PRECISION` and will trigger an error in `checkDecimal`.
        int scale = value[position + 1] & 0xFF;
        BigDecimal result;
        switch (typeInfo) {
            case DECIMAL4:
                result = BigDecimal.valueOf(readLong(value, position + 2, 4), scale);
                checkDecimal(result, MAX_DECIMAL4_PRECISION);
                break;
            case DECIMAL8:
                result = BigDecimal.valueOf(readLong(value, position + 2, 8), scale);
                checkDecimal(result, MAX_DECIMAL8_PRECISION);
                break;
            case DECIMAL16:
                checkIndex(position + 17, value.length);
                byte[] bytes = new byte[16];
                // Copy the bytes reversely because the `BigInteger` constructor expects a big-endian
                // representation.
                for (int i = 0; i < 16; ++i) {
                    bytes[i] = value[position + 17 - i];
                }
                result = new BigDecimal(new BigInteger(bytes), scale);
                checkDecimal(result, MAX_DECIMAL16_PRECISION);
                break;
            default:
                throw unexpectedType(Type.DECIMAL);
        }
        return result.stripTrailingZeros();
    }

    // Get a float value from variant value `value[position...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static float getFloat(byte[] value, int position)
    {
        checkIndex(position, value.length);
        int basicType = value[position] & BASIC_TYPE_MASK;
        int typeInfo = (value[position] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE || typeInfo != FLOAT) {
            throw unexpectedType(Type.FLOAT);
        }
        return Float.intBitsToFloat((int) readLong(value, position + 1, 4));
    }

    // Get a binary value from variant value `value[position...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static byte[] getBinary(byte[] value, int position)
    {
        checkIndex(position, value.length);
        int basicType = value[position] & BASIC_TYPE_MASK;
        int typeInfo = (value[position] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE || typeInfo != BINARY) {
            throw unexpectedType(Type.BINARY);
        }
        int start = position + 1 + U32_SIZE;
        int length = readUnsigned(value, position + 1, U32_SIZE);
        checkIndex(start + length - 1, value.length);
        return Arrays.copyOfRange(value, start, start + length);
    }

    // Get a string value from variant value `value[position...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static String getString(byte[] value, int position)
    {
        checkIndex(position, value.length);
        int basicType = value[position] & BASIC_TYPE_MASK;
        int typeInfo = (value[position] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType == SHORT_STR || (basicType == PRIMITIVE && typeInfo == LONG_STR)) {
            int start;
            int length;
            if (basicType == SHORT_STR) {
                start = position + 1;
                length = typeInfo;
            }
            else {
                start = position + 1 + U32_SIZE;
                length = readUnsigned(value, position + 1, U32_SIZE);
            }
            checkIndex(start + length - 1, value.length);
            return new String(value, start, length, UTF_8);
        }
        throw unexpectedType(Type.STRING);
    }

    public interface ObjectHandler<T>
    {
        /**
         * @param size Number of object fields.
         * @param idSize The integer size of the field id list.
         * @param offsetSize The integer size of the offset list.
         * @param idStart The starting index of the field id list in the variant value array.
         * @param offsetStart The starting index of the offset list in the variant value array.
         * @param dataStart The starting index of field data in the variant value array.
         */
        T apply(int size, int idSize, int offsetSize, int idStart, int offsetStart, int dataStart);
    }

    // A helper function to access a variant object. It provides `handler` with its required
    // parameters and returns what it returns.
    public static <T> T handleObject(byte[] value, int position, ObjectHandler<T> handler)
    {
        checkIndex(position, value.length);
        int basicType = value[position] & BASIC_TYPE_MASK;
        int typeInfo = (value[position] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != OBJECT) {
            throw unexpectedType(Type.OBJECT);
        }
        // Refer to the comment of the `OBJECT` constant for the details of the object header encoding.
        // Suppose `typeInfo` has a bit representation of 0_b4_b3b2_b1b0, the following line extracts
        // b4 to determine whether the object uses a 1/4-byte size.
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = (largeSize ? U32_SIZE : 1);
        int size = readUnsigned(value, position + 1, sizeBytes);
        // Extracts b3b2 to determine the integer size of the field id list.
        int idSize = ((typeInfo >> 2) & 0x3) + 1;
        // Extracts b1b0 to determine the integer size of the offset list.
        int offsetSize = (typeInfo & 0x3) + 1;
        int idStart = position + 1 + sizeBytes;
        int offsetStart = idStart + size * idSize;
        int dataStart = offsetStart + (size + 1) * offsetSize;
        return handler.apply(size, idSize, offsetSize, idStart, offsetStart, dataStart);
    }

    public interface ArrayHandler<T>
    {
        /**
         * @param size Number of array elements.
         * @param offsetSize The integer size of the offset list.
         * @param offsetStart The starting index of the offset list in the variant value array.
         * @param dataStart The starting index of element data in the variant value array.
         */
        T apply(int size, int offsetSize, int offsetStart, int dataStart);
    }

    // A helper function to access a variant array.
    public static <T> T handleArray(byte[] value, int position, ArrayHandler<T> handler)
    {
        checkIndex(position, value.length);
        int basicType = value[position] & BASIC_TYPE_MASK;
        int typeInfo = (value[position] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != ARRAY) {
            throw unexpectedType(Type.ARRAY);
        }
        // Refer to the comment of the `ARRAY` constant for the details of the object header encoding.
        // Suppose `typeInfo` has a bit representation of 000_b2_b1b0, the following line extracts
        // b2 to determine whether the object uses a 1/4-byte size.
        boolean largeSize = ((typeInfo >> 2) & 0x1) != 0;
        int sizeBytes = (largeSize ? U32_SIZE : 1);
        int size = readUnsigned(value, position + 1, sizeBytes);
        // Extracts b1b0 to determine the integer size of the offset list.
        int offsetSize = (typeInfo & 0x3) + 1;
        int offsetStart = position + 1 + sizeBytes;
        int dataStart = offsetStart + (size + 1) * offsetSize;
        return handler.apply(size, offsetSize, offsetStart, dataStart);
    }

    // Get a key at `id` in the variant metadata.
    // Throw `MALFORMED_VARIANT` if the variant is malformed. An out-of-bound `id` is also considered
    // a malformed variant because it is read from the corresponding variant value.
    public static String getMetadataKey(byte[] metadata, int id)
    {
        checkIndex(0, metadata.length);
        // Extracts the highest 2 bits in the metadata header to determine the integer size of the
        // offset list.
        int offsetSize = ((metadata[0] >> 6) & 0x3) + 1;
        int dictSize = readUnsigned(metadata, 1, offsetSize);
        if (id >= dictSize) {
            throw new IllegalArgumentException("Index out of bound: %s (size: %s)".formatted(id, dictSize));
        }
        // There are a header byte, a `dictSize` with `offsetSize` bytes, and `(dictSize + 1)` offsets
        // before the string data.
        int stringStart = 1 + (dictSize + 2) * offsetSize;
        int offset = readUnsigned(metadata, 1 + (id + 1) * offsetSize, offsetSize);
        int nextOffset = readUnsigned(metadata, 1 + (id + 2) * offsetSize, offsetSize);
        if (offset > nextOffset) {
            throw new IllegalArgumentException("Invalid offset: %s > %s".formatted(offset, nextOffset));
        }
        checkIndex(stringStart + nextOffset - 1, metadata.length);
        return new String(metadata, stringStart + offset, nextOffset - offset, UTF_8);
    }
}
