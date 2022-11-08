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
package io.trino.hive.formats.rcfile;

import com.google.errorprone.annotations.FormatMethod;

import java.io.IOException;

import static java.lang.String.format;

public class RcFileCorruptionException
        extends IOException
{
    public RcFileCorruptionException(String message)
    {
        super(message);
    }

    @FormatMethod
    public RcFileCorruptionException(String messageFormat, Object... args)
    {
        super(format(messageFormat, args));
    }

    @FormatMethod
    public RcFileCorruptionException(Throwable cause, String messageFormat, Object... args)
    {
        super(format(messageFormat, args), cause);
    }
}
