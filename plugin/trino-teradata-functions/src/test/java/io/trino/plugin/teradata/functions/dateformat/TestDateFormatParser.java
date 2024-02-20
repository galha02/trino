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
package io.trino.plugin.teradata.functions.dateformat;

import io.trino.spi.TrinoException;
import org.antlr.v4.runtime.Token;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDateFormatParser
{
    @Test
    public void testTokenize()
    {
        assertEquals(
                asList(DateFormat.YYYY, DateFormat.TEXT, DateFormat.MM),
                DateFormatParser.tokenize("yyyy mm").stream().map(Token::getType).collect(Collectors.toList()));
    }

    @Test
    public void testGreedinessLongFirst()
    {
        assertEquals(DateFormatParser.tokenize("yy").size(), 1);
        assertEquals(DateFormatParser.tokenize("yyyy").size(), 1);
        assertEquals(DateFormatParser.tokenize("yyyyyy").size(), 2);
    }

    @Test
    public void testInvalidTokenTokenize()
    {
        assertEquals(
                asList(DateFormat.UNRECOGNIZED, DateFormat.UNRECOGNIZED, DateFormat.UNRECOGNIZED),
                DateFormatParser.tokenize("ala").stream().map(Token::getType).collect(Collectors.toList()));
    }

    @Test
    public void testInvalidTokenCreate1()
    {
        assertThatThrownBy(() -> DateFormatParser.createDateTimeFormatter("ala"))
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testInvalidTokenCreate2()
    {
        assertThatThrownBy(() -> DateFormatParser.createDateTimeFormatter("yyym/mm/dd"))
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testCreateDateTimeFormatter()
    {
        DateTimeFormatter formatter = DateFormatParser.createDateTimeFormatter("yyyy/mm/dd");
        assertEquals(new DateTime(1988, 4, 8, 0, 0), formatter.parseDateTime("1988/04/08"));
    }
}
