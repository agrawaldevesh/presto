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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.DERIVED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_MULT_CHOICE;
import static java.net.HttpURLConnection.HTTP_OK;

public class PinotUtils
{
    private PinotUtils()
    {
    }

    static boolean isValidPinotHttpResponseCode(int status)
    {
        return status >= HTTP_OK && status < HTTP_MULT_CHOICE;
    }

    public static <A, B extends A> B checkType(A value, Class<B> target, String name)
    {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(target.isInstance(value), "%s must be of type %s, not %s", name, target.getName(), value.getClass().getName());
        return target.cast(value);
    }

    public static <T> T doWithRetries(int numRetries, Function<Integer, T> caller)
    {
        PinotException firstError = null;
        Preconditions.checkState(numRetries > 0, "Invalid num of retries %d", numRetries);
        for (int i = 0; i < numRetries; ++i) {
            try {
                return caller.apply(i);
            }
            catch (PinotException e) {
                if (firstError == null) {
                    firstError = e;
                }
                if (!e.getPinotErrorCode().isRetriable()) {
                    throw e;
                }
            }
        }
        throw firstError;
    }
}
