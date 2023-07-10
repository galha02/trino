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

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum HiveErrorCode
        implements ErrorCodeSupplier
{
    HIVE_METASTORE_ERROR(0, EXTERNAL),
    HIVE_CURSOR_ERROR(1, EXTERNAL),
    HIVE_TABLE_OFFLINE(2, USER_ERROR),
    HIVE_CANNOT_OPEN_SPLIT(3, EXTERNAL),
    HIVE_FILE_NOT_FOUND(4, EXTERNAL),
    HIVE_UNKNOWN_ERROR(5, EXTERNAL),
    HIVE_PARTITION_OFFLINE(6, USER_ERROR),
    HIVE_BAD_DATA(7, EXTERNAL),
    HIVE_PARTITION_SCHEMA_MISMATCH(8, EXTERNAL),
    // HIVE_MISSING_DATA(9, EXTERNAL) is deprecated
    HIVE_INVALID_PARTITION_VALUE(10, EXTERNAL),
    HIVE_TIMEZONE_MISMATCH(11, EXTERNAL),
    HIVE_INVALID_METADATA(12, EXTERNAL),
    HIVE_INVALID_VIEW_DATA(13, EXTERNAL),
    HIVE_DATABASE_LOCATION_ERROR(14, EXTERNAL),
    HIVE_PATH_ALREADY_EXISTS(15, EXTERNAL),
    HIVE_FILESYSTEM_ERROR(16, EXTERNAL),
    // code HIVE_WRITER_ERROR(17) is deprecated
    HIVE_SERDE_NOT_FOUND(18, EXTERNAL),
    HIVE_UNSUPPORTED_FORMAT(19, EXTERNAL),
    HIVE_PARTITION_READ_ONLY(20, USER_ERROR),
    HIVE_TOO_MANY_OPEN_PARTITIONS(21, USER_ERROR),
    HIVE_CONCURRENT_MODIFICATION_DETECTED(22, EXTERNAL),
    HIVE_COLUMN_ORDER_MISMATCH(23, USER_ERROR),
    HIVE_FILE_MISSING_COLUMN_NAMES(24, EXTERNAL),
    HIVE_WRITER_OPEN_ERROR(25, EXTERNAL),
    HIVE_WRITER_CLOSE_ERROR(26, EXTERNAL),
    HIVE_WRITER_DATA_ERROR(27, EXTERNAL),
    HIVE_INVALID_BUCKET_FILES(28, EXTERNAL),
    HIVE_EXCEEDED_PARTITION_LIMIT(29, USER_ERROR),
    HIVE_WRITE_VALIDATION_FAILED(30, INTERNAL_ERROR),
    HIVE_PARTITION_DROPPED_DURING_QUERY(31, EXTERNAL),
    HIVE_TABLE_READ_ONLY(32, USER_ERROR),
    HIVE_PARTITION_NOT_READABLE(33, USER_ERROR),
    HIVE_TABLE_NOT_READABLE(34, USER_ERROR),
    HIVE_TABLE_DROPPED_DURING_QUERY(35, EXTERNAL),
    // HIVE_TOO_MANY_BUCKET_SORT_FILES(36) is deprecated
    HIVE_CORRUPTED_COLUMN_STATISTICS(37, EXTERNAL),
    HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT(38, USER_ERROR),
    HIVE_UNKNOWN_COLUMN_STATISTIC_TYPE(39, INTERNAL_ERROR),
    HIVE_TABLE_LOCK_NOT_ACQUIRED(40, EXTERNAL),
    HIVE_VIEW_TRANSLATION_ERROR(41, EXTERNAL),
    HIVE_PARTITION_NOT_FOUND(42, USER_ERROR),
    /**/;

    private final ErrorCode errorCode;

    HiveErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0100_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
