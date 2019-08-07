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
package io.prestosql.plugin.prometheus;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.EXTERNAL;
import static io.prestosql.spi.ErrorType.USER_ERROR;

public enum PrometheusErrorCode
        implements ErrorCodeSupplier
{
    PROMETHEUS_UNKNOWN_ERROR(0, EXTERNAL),
    PROMETHEUS_TABLES_METRICS_RETRIEVE_ERROR(1, USER_ERROR),
    PROMETHEUS_PARSE_ERROR(2, EXTERNAL);

    private final ErrorCode errorCode;

    PrometheusErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0509_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
