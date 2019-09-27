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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.PinotSessionProperties;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.query.PinotExpression.derived;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Convert {@link com.facebook.presto.spi.relation.RowExpression} in project into Pinot compliant expression text
 */
class PinotProjectExpressionConverter
        implements RowExpressionVisitor<PinotExpression, Map<VariableReferenceExpression, Selection>>
{
    // Pinot does not support modulus yet
    private static final Map<String, String> PRESTO_TO_PINOT_OP = ImmutableMap.of(
            "-", "SUB",
            "+", "ADD",
            "*", "MULT",
            "/", "DIV");

    private static final Set<String> PROSCRIBED_FUNCTIONS = ImmutableSet.of("not", "between");

    private static final Set<String> TIME_EQUIVALENT_TYPES = ImmutableSet.of(StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.TINYINT, StandardTypes.SMALLINT);

    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final ConnectorSession session;

    public PinotProjectExpressionConverter(TypeManager typeManager, FunctionMetadataManager functionMetadataManager, ConnectorSession session)
    {
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.session = session;
    }

    @Override
    public PinotExpression visitVariableReference(VariableReferenceExpression reference, Map<VariableReferenceExpression, Selection> context)
    {
        Selection input = requireNonNull(context.get(reference), format("Input column %s does not exist in the input", reference));
        return new PinotExpression(input.getDefinition(), input.getOrigin());
    }

    private CallExpression getExpressionAsFunction(RowExpression originalExpression, RowExpression expression, Map<VariableReferenceExpression, Selection> context)
    {
        if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            if (call.getDisplayName().equalsIgnoreCase("cast")) {
                if (isImplicitCast(call.getArguments().get(0).getType(), call.getType())) {
                    return getExpressionAsFunction(originalExpression, call.getArguments().get(0), context);
                }
            }
            else {
                return call;
            }
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Could not dig function out of expression: " + originalExpression + ", inside of " + expression);
    }

    private PinotExpression handleDateTruncViaDateTimeConvert(CallExpression function, Map<VariableReferenceExpression, Selection> context)
    {
        // Convert SQL standard function `DATE_TRUNC(INTERVAL, DATE/TIMESTAMP COLUMN)` to
        // Pinot's equivalent function `dateTimeConvert(columnName, inputFormat, outputFormat, outputGranularity)`
        // Pinot doesn't have a DATE/TIMESTAMP type. That means the input column (second argument) has been converted from numeric type to DATE/TIMESTAMP using one of the
        // conversion functions in SQL. First step is find the function and find its input column units (seconds, secondsSinceEpoch etc.)
        RowExpression timeInputParameter = function.getArguments().get(1);
        String inputColumn;
        String inputFormat;

        CallExpression timeConversion = getExpressionAsFunction(timeInputParameter, timeInputParameter, context);
        switch (timeConversion.getDisplayName().toLowerCase(ENGLISH)) {
            case "from_unixtime":
                inputColumn = timeConversion.getArguments().get(0).accept(this, context).getDefinition();
                inputFormat = "'1:SECONDS:EPOCH'";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "not supported: " + timeConversion.getDisplayName());
        }

        String outputFormat = "'1:MILLISECONDS:EPOCH'";
        String outputGranularity;

        RowExpression intervalParameter = function.getArguments().get(0);
        if (!(intervalParameter instanceof ConstantExpression)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                    "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        String strValue = getStringFromConstant(intervalParameter);
        switch (strValue) {
            case "second":
                outputGranularity = "'1:SECONDS'";
                break;
            case "minute":
                outputGranularity = "'1:MINUTES'";
                break;
            case "hour":
                outputGranularity = "'1:HOURS'";
                break;
            case "day":
                outputGranularity = "'1:DAYS'";
                break;
            case "week":
                outputGranularity = "'1:WEEKS'";
                break;
            case "month":
                outputGranularity = "'1:MONTHS'";
                break;
            case "quarter":
                outputGranularity = "'1:QUARTERS'";
                break;
            case "year":
                outputGranularity = "'1:YEARS'";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                        "interval in date_trunc is not supported: " + strValue);
        }

        return derived("dateTimeConvert(" + inputColumn + ", " + inputFormat + ", " + outputFormat + ", " + outputGranularity + ")");
    }

    private static String getStringFromConstant(RowExpression expression)
    {
        if (expression instanceof ConstantExpression) {
            Object value = ((ConstantExpression) expression).getValue();
            if (value instanceof String) {
                return (String) value;
            }
            else if (value instanceof Slice) {
                return ((Slice) value).toStringUtf8();
            }
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Expected string literal but found " + expression);
    }

    private PinotExpression handleDateTruncViaPrestoDateTrunc(CallExpression function, Map<VariableReferenceExpression, Selection> context)
    {
        RowExpression timeInputParameter = function.getArguments().get(1);
        String inputColumn;
        String inputTimeZone;
        String inputFormat;

        CallExpression timeConversion = getExpressionAsFunction(timeInputParameter, timeInputParameter, context);
        switch (timeConversion.getDisplayName().toLowerCase(ENGLISH)) {
            case "from_unixtime":
                inputColumn = timeConversion.getArguments().get(0).accept(this, context).getDefinition();
                inputTimeZone = timeConversion.getArguments().size() > 1 ? getStringFromConstant(timeConversion.getArguments().get(1)) : DateTimeZone.UTC.getID();
                inputFormat = "seconds";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "not supported: " + timeConversion.getDisplayName());
        }

        RowExpression intervalParameter = function.getArguments().get(0);
        if (!(intervalParameter instanceof ConstantExpression)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                    "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        return derived("prestoDateTrunc(" + inputColumn + "," + inputFormat + ", " + inputTimeZone + ", " + getStringFromConstant(intervalParameter) + ")");
    }

    @Override
    public PinotExpression visitConstant(ConstantExpression literal, Map<VariableReferenceExpression, Selection> context)
    {
        return new PinotExpression(literal.getValue().toString(), Origin.LITERAL);
    }

    @Override
    public PinotExpression visitLambda(LambdaDefinitionExpression lambda, Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Pinot does not support lambda " + lambda);
    }

    private boolean isImplicitCast(Type inputType, Type resultType)
    {
        if (typeManager.canCoerce(inputType, resultType)) {
            return true;
        }
        return resultType.getTypeSignature().getBase().equals(StandardTypes.TIMESTAMP) && TIME_EQUIVALENT_TYPES.contains(inputType.getTypeSignature().getBase());
    }

    private PinotExpression handleArithmeticExpression(CallExpression expression, OperatorType operatorType, Map<VariableReferenceExpression, Selection> context)
    {
        List<RowExpression> arguments = expression.getArguments();
        if (arguments.size() == 1) {
            String prefix = operatorType == OperatorType.NEGATION ? "-" : "";
            return derived(prefix + arguments.get(0).accept(this, context).getDefinition());
        }
        else if (arguments.size() == 2) {
            PinotExpression left = arguments.get(0).accept(this, context);
            PinotExpression right = arguments.get(1).accept(this, context);
            String prestoOp = operatorType.getOperator();
            String pinotOp = PRESTO_TO_PINOT_OP.get(prestoOp);
            if (pinotOp == null) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unsupported binary expression " + prestoOp);
            }
            return derived(format("%s(%s, %s)", pinotOp, left.getDefinition(), right.getDefinition()));
        }
        else {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Don't know how to interpret %s as an arithmetic expression", expression));
        }
    }

    private PinotExpression handleCast(CallExpression cast, Map<VariableReferenceExpression, Selection> context)
    {
        if (cast.getArguments().size() == 1) {
            RowExpression input = cast.getArguments().get(0);
            Type expectedType = cast.getType();
            if (isImplicitCast(input.getType(), expectedType)) {
                return input.accept(this, context);
            }
            else {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Non implicit casts not supported: " + cast);
            }
        }

        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("This type of CAST operator not supported. Received: %s", cast));
    }

    private PinotExpression handleFunction(CallExpression function, Map<VariableReferenceExpression, Selection> context)
    {
        switch (function.getDisplayName().toLowerCase(ENGLISH)) {
            case "date_trunc":
                boolean usePrestoDateTrunc = PinotSessionProperties.isUsePrestoDateTrunc(session);
                return usePrestoDateTrunc ?
                        handleDateTruncViaPrestoDateTrunc(function, context) :
                        handleDateTruncViaDateTimeConvert(function, context);
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("function %s not supported yet", function.getDisplayName()));
        }
    }

    @Override
    public PinotExpression visitCall(CallExpression call, Map<VariableReferenceExpression, Selection> context)
    {
        String displayName = call.getDisplayName().toLowerCase(ENGLISH);
        if (PROSCRIBED_FUNCTIONS.contains(displayName)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unsupported function in pinot aggregation: " + call);
        }
        if (displayName.equals("cast")) {
            return handleCast(call, context);
        }
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType.isArithmeticOperator()) {
                return handleArithmeticExpression(call, operatorType, context);
            }
            else if (operatorType.isComparisonOperator()) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Comparison operator not supported: " + call);
            }
        }
        return handleFunction(call, context);
    }

    @Override
    public PinotExpression visitInputReference(InputReferenceExpression reference, Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Input reference not supported: " + reference);
    }

    @Override
    public PinotExpression visitSpecialForm(SpecialFormExpression specialForm, Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Special form not supported: " + specialForm);
    }
}
