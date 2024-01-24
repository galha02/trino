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
package io.trino.plugin.varada.expression.rewrite.worker.varadatolucene;

import io.trino.matching.Pattern;
import io.trino.plugin.varada.expression.VaradaExpression;
import org.apache.lucene.search.BooleanClause;

interface ExpressionRewriter<T extends VaradaExpression>
{
    Pattern<T> getPattern();

    default LuceneRewriteContext createContext(LuceneRewriteContext context, BooleanClause.Occur occur)
    {
        return new LuceneRewriteContext(context.queryBuilder(), occur, context.collectNulls());
    }
}