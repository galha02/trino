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
package io.prestosql.elasticsearch;

import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

public final class ElasticsearchQueryBuilder
{
    private ElasticsearchQueryBuilder() {}

    public static QueryBuilder buildSearchQuery(ConnectorSession session, TupleDomain<ElasticsearchColumnHandle> constraint, Optional<String> query)
    {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        if (constraint.getDomains().isPresent()) {
            for (Map.Entry<ElasticsearchColumnHandle, Domain> entry : constraint.getDomains().get().entrySet()) {
                ElasticsearchColumnHandle column = entry.getKey();
                Domain domain = entry.getValue();

                checkArgument(!domain.isNone(), "Unexpected NONE domain for %s", column.getName());
                if (!domain.isAll()) {
                    queryBuilder.filter(new BoolQueryBuilder().must(buildPredicate(session, column.getName(), domain, column.getType())));
                }
            }
        }
        query.map(QueryStringQueryBuilder::new)
                .ifPresent(queryBuilder::must);

        if (queryBuilder.hasClauses()) {
            return queryBuilder;
        }
        return new MatchAllQueryBuilder();
    }

    private static QueryBuilder buildPredicate(ConnectorSession session, String columnName, Domain domain, Type type)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        if (domain.getValues().isNone()) {
            boolQueryBuilder.mustNot(new ExistsQueryBuilder(columnName));
            return boolQueryBuilder;
        }

        if (domain.getValues().isAll()) {
            boolQueryBuilder.must(new ExistsQueryBuilder(columnName));
            return boolQueryBuilder;
        }

        return buildTermQuery(boolQueryBuilder, session, columnName, domain, type);
    }

    private static QueryBuilder buildTermQuery(BoolQueryBuilder queryBuilder, ConnectorSession session, String columnName, Domain domain, Type type)
    {
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            BoolQueryBuilder rangeQueryBuilder = new BoolQueryBuilder();
            Set<Object> valuesToInclude = new HashSet<>();
            checkState(!range.isAll(), "Invalid range for column: " + columnName);
            if (range.isSingleValue()) {
                valuesToInclude.add(range.getLow().getValue());
            }
            else {
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeQueryBuilder.filter(new RangeQueryBuilder(columnName).gt(getValue(session, type, range.getLow().getValue())));
                            break;
                        case EXACTLY:
                            rangeQueryBuilder.filter(new RangeQueryBuilder(columnName).gte(getValue(session, type, range.getLow().getValue())));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case EXACTLY:
                            rangeQueryBuilder.filter(new RangeQueryBuilder(columnName).lte(getValue(session, type, range.getHigh().getValue())));
                            break;
                        case BELOW:
                            rangeQueryBuilder.filter(new RangeQueryBuilder(columnName).lt(getValue(session, type, range.getHigh().getValue())));
                            break;
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
            }

            if (valuesToInclude.size() == 1) {
                rangeQueryBuilder.filter(new TermQueryBuilder(columnName, getValue(session, type, getOnlyElement(valuesToInclude))));
            }
            queryBuilder.should(rangeQueryBuilder);
        }
        return queryBuilder;
    }

    private static Object getValue(ConnectorSession session, Type type, Object value)
    {
        if (type.equals(BOOLEAN) ||
                type.equals(TINYINT) ||
                type.equals(SMALLINT) ||
                type.equals(INTEGER) ||
                type.equals(BIGINT) ||
                type.equals(DOUBLE)) {
            return value;
        }
        if (type.equals(REAL)) {
            return Float.intBitsToFloat(toIntExact(((Long) value)));
        }
        if (type.equals(VARCHAR)) {
            return ((Slice) value).toStringUtf8();
        }
        if (type.equals(TIMESTAMP)) {
            checkState(session.isLegacyTimestamp(), "New timestamp semantics not yet supported");

            return Instant.ofEpochMilli((Long) value)
                    .atZone(ZoneId.of(session.getTimeZoneKey().getId()))
                    .toLocalDateTime()
                    .format(ISO_DATE_TIME);
        }
        throw new IllegalArgumentException("Unhandled type: " + type);
    }
}
