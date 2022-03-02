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
package io.trino.spi;

import static io.trino.spi.ErrorType.INSUFFICIENT_RESOURCES;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum StandardErrorCode
        implements ErrorCodeSupplier
{
    GENERIC_USER_ERROR(0, USER_ERROR),
    SYNTAX_ERROR(1, USER_ERROR),
    ABANDONED_QUERY(2, USER_ERROR),
    USER_CANCELED(3, USER_ERROR),
    PERMISSION_DENIED(4, USER_ERROR),
    NOT_FOUND(5, USER_ERROR),
    FUNCTION_NOT_FOUND(6, USER_ERROR),
    INVALID_FUNCTION_ARGUMENT(7, USER_ERROR),             // caught by TRY
    DIVISION_BY_ZERO(8, USER_ERROR),                      // caught by TRY
    INVALID_CAST_ARGUMENT(9, USER_ERROR),                 // caught by TRY
    OPERATOR_NOT_FOUND(10, USER_ERROR),
    INVALID_VIEW(11, USER_ERROR),
    ALREADY_EXISTS(12, USER_ERROR),
    NOT_SUPPORTED(13, USER_ERROR),
    INVALID_SESSION_PROPERTY(14, USER_ERROR),
    INVALID_WINDOW_FRAME(15, USER_ERROR),
    CONSTRAINT_VIOLATION(16, USER_ERROR),
    TRANSACTION_CONFLICT(17, USER_ERROR),
    INVALID_TABLE_PROPERTY(18, USER_ERROR),
    NUMERIC_VALUE_OUT_OF_RANGE(19, USER_ERROR),            // caught by TRY
    UNKNOWN_TRANSACTION(20, USER_ERROR),
    NOT_IN_TRANSACTION(21, USER_ERROR),
    TRANSACTION_ALREADY_ABORTED(22, USER_ERROR),
    READ_ONLY_VIOLATION(23, USER_ERROR),
    MULTI_CATALOG_WRITE_CONFLICT(24, USER_ERROR),
    AUTOCOMMIT_WRITE_CONFLICT(25, USER_ERROR),
    UNSUPPORTED_ISOLATION_LEVEL(26, USER_ERROR),
    INCOMPATIBLE_CLIENT(27, USER_ERROR),
    SUBQUERY_MULTIPLE_ROWS(28, USER_ERROR),
    PROCEDURE_NOT_FOUND(29, USER_ERROR),
    INVALID_PROCEDURE_ARGUMENT(30, USER_ERROR),
    QUERY_REJECTED(31, USER_ERROR),
    AMBIGUOUS_FUNCTION_CALL(32, USER_ERROR),
    INVALID_SCHEMA_PROPERTY(33, USER_ERROR),
    SCHEMA_NOT_EMPTY(34, USER_ERROR),
    QUERY_TEXT_TOO_LARGE(35, USER_ERROR),
    UNSUPPORTED_SUBQUERY(36, USER_ERROR),
    EXCEEDED_FUNCTION_MEMORY_LIMIT(37, USER_ERROR),
    ADMINISTRATIVELY_KILLED(38, USER_ERROR),
    INVALID_COLUMN_PROPERTY(39, USER_ERROR),
    QUERY_HAS_TOO_MANY_STAGES(40, USER_ERROR),
    INVALID_SPATIAL_PARTITIONING(41, USER_ERROR),
    INVALID_ANALYZE_PROPERTY(42, USER_ERROR),
    TYPE_NOT_FOUND(43, USER_ERROR),
    CATALOG_NOT_FOUND(44, USER_ERROR),
    SCHEMA_NOT_FOUND(45, USER_ERROR),
    TABLE_NOT_FOUND(46, USER_ERROR),
    COLUMN_NOT_FOUND(47, USER_ERROR),
    ROLE_NOT_FOUND(48, USER_ERROR),
    SCHEMA_ALREADY_EXISTS(49, USER_ERROR),
    TABLE_ALREADY_EXISTS(50, USER_ERROR),
    COLUMN_ALREADY_EXISTS(51, USER_ERROR),
    ROLE_ALREADY_EXISTS(52, USER_ERROR),
    DUPLICATE_NAMED_QUERY(53, USER_ERROR),
    DUPLICATE_COLUMN_NAME(54, USER_ERROR),
    MISSING_COLUMN_NAME(55, USER_ERROR),
    MISSING_CATALOG_NAME(56, USER_ERROR),
    MISSING_SCHEMA_NAME(57, USER_ERROR),
    TYPE_MISMATCH(58, USER_ERROR),
    INVALID_LITERAL(59, USER_ERROR),
    COLUMN_TYPE_UNKNOWN(60, USER_ERROR),
    MISMATCHED_COLUMN_ALIASES(61, USER_ERROR),
    AMBIGUOUS_NAME(62, USER_ERROR),
    INVALID_COLUMN_REFERENCE(63, USER_ERROR),
    MISSING_GROUP_BY(64, USER_ERROR),
    MISSING_ORDER_BY(65, USER_ERROR),
    MISSING_OVER(66, USER_ERROR),
    NESTED_AGGREGATION(67, USER_ERROR),
    NESTED_WINDOW(68, USER_ERROR),
    EXPRESSION_NOT_IN_DISTINCT(69, USER_ERROR),
    TOO_MANY_GROUPING_SETS(70, USER_ERROR),
    FUNCTION_NOT_WINDOW(71, USER_ERROR),
    FUNCTION_NOT_AGGREGATE(72, USER_ERROR),
    EXPRESSION_NOT_AGGREGATE(73, USER_ERROR),
    EXPRESSION_NOT_SCALAR(74, USER_ERROR),
    EXPRESSION_NOT_CONSTANT(75, USER_ERROR),
    INVALID_ARGUMENTS(76, USER_ERROR),
    TOO_MANY_ARGUMENTS(77, USER_ERROR),
    INVALID_PRIVILEGE(78, USER_ERROR),
    DUPLICATE_PROPERTY(79, USER_ERROR),
    INVALID_PARAMETER_USAGE(80, USER_ERROR),
    VIEW_IS_STALE(81, USER_ERROR),
    VIEW_IS_RECURSIVE(82, USER_ERROR),
    NULL_TREATMENT_NOT_ALLOWED(83, USER_ERROR),
    INVALID_ROW_FILTER(84, USER_ERROR),
    INVALID_COLUMN_MASK(85, USER_ERROR),
    MISSING_TABLE(86, USER_ERROR),
    INVALID_RECURSIVE_REFERENCE(87, USER_ERROR),
    MISSING_COLUMN_ALIASES(88, USER_ERROR),
    NESTED_RECURSIVE(89, USER_ERROR),
    INVALID_LIMIT_CLAUSE(90, USER_ERROR),
    INVALID_ORDER_BY(91, USER_ERROR),
    DUPLICATE_WINDOW_NAME(92, USER_ERROR),
    INVALID_WINDOW_REFERENCE(93, USER_ERROR),
    INVALID_PARTITION_BY(94, USER_ERROR),
    INVALID_MATERIALIZED_VIEW_PROPERTY(95, USER_ERROR),
    INVALID_LABEL(96, USER_ERROR),
    INVALID_PROCESSING_MODE(97, USER_ERROR),
    INVALID_NAVIGATION_NESTING(98, USER_ERROR),
    INVALID_ROW_PATTERN(99, USER_ERROR),
    NESTED_ROW_PATTERN_RECOGNITION(100, USER_ERROR),
    TABLE_HAS_NO_COLUMNS(101, USER_ERROR),
    INVALID_RANGE(102, USER_ERROR),
    INVALID_PATTERN_RECOGNITION_FUNCTION(103, USER_ERROR),
    TABLE_REDIRECTION_ERROR(104, USER_ERROR),
    MISSING_VARIABLE_DEFINITIONS(105, USER_ERROR),
    MISSING_ROW_PATTERN(106, USER_ERROR),
    INVALID_WINDOW_MEASURE(107, USER_ERROR),
    STACK_OVERFLOW(108, USER_ERROR),

    GENERIC_INTERNAL_ERROR(65536, INTERNAL_ERROR),
    TOO_MANY_REQUESTS_FAILED(65537, INTERNAL_ERROR),
    PAGE_TOO_LARGE(65538, INTERNAL_ERROR),
    PAGE_TRANSPORT_ERROR(65539, INTERNAL_ERROR),
    PAGE_TRANSPORT_TIMEOUT(65540, INTERNAL_ERROR),
    NO_NODES_AVAILABLE(65541, INTERNAL_ERROR),
    REMOTE_TASK_ERROR(65542, INTERNAL_ERROR),
    COMPILER_ERROR(65543, INTERNAL_ERROR),
    REMOTE_TASK_MISMATCH(65544, INTERNAL_ERROR),
    SERVER_SHUTTING_DOWN(65545, INTERNAL_ERROR),
    FUNCTION_IMPLEMENTATION_MISSING(65546, INTERNAL_ERROR),
    REMOTE_BUFFER_CLOSE_FAILED(65547, INTERNAL_ERROR),
    SERVER_STARTING_UP(65548, INTERNAL_ERROR),
    FUNCTION_IMPLEMENTATION_ERROR(65549, INTERNAL_ERROR),
    INVALID_PROCEDURE_DEFINITION(65550, INTERNAL_ERROR),
    PROCEDURE_CALL_FAILED(65551, INTERNAL_ERROR),
    AMBIGUOUS_FUNCTION_IMPLEMENTATION(65552, INTERNAL_ERROR),
    ABANDONED_TASK(65553, INTERNAL_ERROR),
    CORRUPT_SERIALIZED_IDENTITY(65554, INTERNAL_ERROR),
    CORRUPT_PAGE(65555, INTERNAL_ERROR),
    OPTIMIZER_TIMEOUT(65556, INTERNAL_ERROR),
    OUT_OF_SPILL_SPACE(65557, INTERNAL_ERROR),
    REMOTE_HOST_GONE(65558, INTERNAL_ERROR),
    CONFIGURATION_INVALID(65559, INTERNAL_ERROR),
    CONFIGURATION_UNAVAILABLE(65560, INTERNAL_ERROR),
    INVALID_RESOURCE_GROUP(65561, INTERNAL_ERROR),
    SERIALIZATION_ERROR(65562, INTERNAL_ERROR),
    REMOTE_TASK_FAILED(65563, INTERNAL_ERROR),
    EXCHANGE_MANAGER_NOT_CONFIGURED(65564, INTERNAL_ERROR),

    GENERIC_INSUFFICIENT_RESOURCES(131072, INSUFFICIENT_RESOURCES),
    EXCEEDED_GLOBAL_MEMORY_LIMIT(131073, INSUFFICIENT_RESOURCES),
    QUERY_QUEUE_FULL(131074, INSUFFICIENT_RESOURCES),
    EXCEEDED_TIME_LIMIT(131075, INSUFFICIENT_RESOURCES),
    CLUSTER_OUT_OF_MEMORY(131076, INSUFFICIENT_RESOURCES),
    EXCEEDED_CPU_LIMIT(131077, INSUFFICIENT_RESOURCES),
    EXCEEDED_SPILL_LIMIT(131078, INSUFFICIENT_RESOURCES),
    EXCEEDED_LOCAL_MEMORY_LIMIT(131079, INSUFFICIENT_RESOURCES),
    ADMINISTRATIVELY_PREEMPTED(131080, INSUFFICIENT_RESOURCES),
    EXCEEDED_SCAN_LIMIT(131081, INSUFFICIENT_RESOURCES),
    EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY(131082, INSUFFICIENT_RESOURCES),

    /**/;

    // Connectors can use error codes starting at the range 0x0100_0000
    // See https://github.com/trinodb/trino/wiki/Error-Codes

    private final ErrorCode errorCode;

    StandardErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
