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
package io.prestosql.matching.pattern;

import io.prestosql.matching.Captures;
import io.prestosql.matching.Match;
import io.prestosql.matching.Matcher;
import io.prestosql.matching.Pattern;
import io.prestosql.matching.PatternVisitor;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class FilterPattern<T>
        extends Pattern<T>
{
    private final Predicate<? super T> predicate;

    public FilterPattern(Predicate<? super T> predicate, Optional<Pattern<?>> previous)
    {
        super(previous);
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    public Predicate<? super T> predicate()
    {
        return predicate;
    }

    @Override
    public Stream<Match<T>> accept(Matcher matcher, Object object, Captures captures)
    {
        return matcher.matchFilter(this, object, captures);
    }

    @Override
    public void accept(PatternVisitor patternVisitor)
    {
        patternVisitor.visitFilter(this);
    }
}
