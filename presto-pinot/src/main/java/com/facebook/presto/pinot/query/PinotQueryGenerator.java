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

import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotConnection;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.pinot.PushdownUtils;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.LITERAL;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.TABLE_COLUMN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PinotQueryGenerator
{
    private static final Map<String, String> UNARY_AGGREGATION_MAP = ImmutableMap.of(
            "min", "min",
            "max", "max",
            "avg", "avg",
            "sum", "sum",
            "approx_distinct", "DISTINCTCOUNTHLL");

    private final PinotConfig pinotConfig;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final PinotFilterExpressionConverter pinotFilterExpressionConverter;

    @Inject
    public PinotQueryGenerator(PinotConfig pinotConfig, TypeManager typeManager, FunctionMetadataManager functionMetadataManager)
    {
        this.pinotConfig = pinotConfig;
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.pinotFilterExpressionConverter = new PinotFilterExpressionConverter(this.typeManager, this.functionMetadataManager);
    }

    public static class PinotQueryGeneratorResult
    {
        private final GeneratedPql generatedPql;
        private final PinotQueryGeneratorContext context;

        public PinotQueryGeneratorResult(GeneratedPql generatedPql, PinotQueryGeneratorContext context)
        {
            this.generatedPql = generatedPql;
            this.context = context;
        }

        public GeneratedPql getGeneratedPql()
        {
            return generatedPql;
        }

        public PinotQueryGeneratorContext getContext()
        {
            return context;
        }
    }

    public PinotQueryGeneratorResult generateForSingleBrokerRequest(PlanNode plan, ConnectorSession session)
    {
        return generateHelper(plan, new PinotPushDownPipelineConverter(Optional.empty(), Optional.empty(), session));
    }

    public PinotQueryGeneratorResult generateForSegmentSplits(PlanNode plan, Optional<String> tableNameSuffix,
            Optional<String> timeBoundaryFilter, ConnectorSession session)
    {
        return generateHelper(plan, new PinotPushDownPipelineConverter(tableNameSuffix, timeBoundaryFilter, session));
    }

    private PinotQueryGeneratorResult generateHelper(PlanNode plan, PinotPushDownPipelineConverter visitor)
    {
        PinotQueryGeneratorContext context = requireNonNull(plan.accept(visitor, null), "Resulting context is null");
        return new PinotQueryGeneratorResult(context.toQuery(pinotConfig), context);
    }

    public static class GeneratedPql
    {
        final String table;
        final String pql;
        final List<Integer> columnIndicesExpected;
        final int numGroupByClauses;

        @JsonCreator
        public GeneratedPql(@JsonProperty("table") String table, @JsonProperty("pql") String pql, @JsonProperty("columnIndicesExpected") List<Integer> columnIndicesExpected, @JsonProperty("numGroupByClauses") int numGroupByClauses)
        {
            this.table = table;
            this.pql = pql;
            this.columnIndicesExpected = columnIndicesExpected;
            this.numGroupByClauses = numGroupByClauses;
        }

        @JsonProperty("pql")
        public String getPql()
        {
            return pql;
        }

        @JsonProperty("columnIndicesExpected")
        public List<Integer> getColumnIndicesExpected()
        {
            return columnIndicesExpected;
        }

        @JsonProperty("numGroupByClauses")
        public int getNumGroupByClauses()
        {
            return numGroupByClauses;
        }

        @JsonProperty("table")
        public String getTable()
        {
            return table;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pql", pql)
                    .add("table", table)
                    .add("columnIndicesExpected", columnIndicesExpected)
                    .add("numGroupByClauses", numGroupByClauses)
                    .toString();
        }
    }

    class PinotPushDownPipelineConverter
            extends PlanVisitor<PinotQueryGeneratorContext, PinotQueryGeneratorContext>
    {
        private final Optional<String> tableNameSuffix;
        private final Optional<String> timeBoundaryFilter;
        private final ConnectorSession session;

        public PinotPushDownPipelineConverter(Optional<String> tableNameSuffix, Optional<String> timeBoundaryFilter, ConnectorSession session)
        {
            this.tableNameSuffix = tableNameSuffix;
            this.timeBoundaryFilter = timeBoundaryFilter;
            this.session = session;
        }

        private String handleAggregationFunction(CallExpression aggregation, Map<VariableReferenceExpression, Selection> inputSelections)
        {
            String prestoAgg = aggregation.getDisplayName().toLowerCase(ENGLISH);
            List<RowExpression> params = aggregation.getArguments();
            switch (prestoAgg) {
                case "count":
                    if (params.size() <= 1) {
                        return format("count(%s)", params.isEmpty() ? "*" : inputSelections.get(getVariableReference(params.get(0))));
                    }
                    break;
                case "approx_percentile":
                    return handleApproxPercentile(aggregation, inputSelections);
                default:
                    if (UNARY_AGGREGATION_MAP.containsKey(prestoAgg) && aggregation.getArguments().size() == 1) {
                        return format("%s(%s)", UNARY_AGGREGATION_MAP.get(prestoAgg), inputSelections.get(getVariableReference(params.get(0))));
                    }
            }

            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("aggregation function '%s' not supported yet", aggregation));
        }

        private VariableReferenceExpression getVariableReference(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                return ((VariableReferenceExpression) expression);
            }
            else {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Expected a variable reference but got " + expression);
            }
        }

        private String handleApproxPercentile(CallExpression aggregation, Map<VariableReferenceExpression, Selection> inputSelections)
        {
            List<RowExpression> inputs = aggregation.getArguments();
            if (inputs.size() != 2) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Cannot handle approx_percentile function " + aggregation);
            }

            Selection fraction = inputSelections.get(getVariableReference(inputs.get(1)));
            if (fraction.getOrigin() != LITERAL) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                        "Cannot handle approx_percentile percentage argument be a non literal " + aggregation);
            }

            int percentile = getValidPercentile(fraction.getDefinition());
            if (percentile < 0 || percentile > 100) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                        format("Cannot handle approx_percentile parsed as %d from input %s (function %s)", percentile, fraction, aggregation));
            }
            return format("PERCENTILEEST%d(%s)", percentile, inputSelections.get(getVariableReference(inputs.get(0))));
        }

        private int getValidPercentile(String fraction)
        {
            try {
                double percent = Double.parseDouble(fraction) * 100.0;
                if (percent >= 0 && percent <= 100 && percent == Math.floor(percent)) {
                    return (int) percent;
                }
            }
            catch (NumberFormatException ne) {
                // Skip
            }
            return -1;
        }

        @Override
        public PinotQueryGeneratorContext visitPlan(PlanNode node, PinotQueryGeneratorContext context)
        {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Don't know how to handle plan node of type " + node);
        }

        @Override
        public PinotQueryGeneratorContext visitAggregation(AggregationNode node, PinotQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            checkArgument(!node.getStep().isOutputPartial(), "partial aggregations are not supported in Pinot pushdown framework");

            LinkedHashMap<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();
            LinkedHashSet<VariableReferenceExpression> groupByColumns = new LinkedHashSet<>();
            HashSet<VariableReferenceExpression> hiddenColumnSet = new HashSet<>(context.getHiddenColumnSet());
            int numAggregations = 0;
            boolean groupByExists = false;

            for (PushdownUtils.AggregationColumnNode expr : PushdownUtils.computeAggregationNodes(node).orElseThrow(() -> new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unsupported aggregation node " + node))) {
                switch (expr.getExprType()) {
                    case GROUP_BY: {
                        PushdownUtils.GroupByColumnNode groupByColumn = (PushdownUtils.GroupByColumnNode) expr;
                        VariableReferenceExpression groupByInputColumn = getVariableReference(groupByColumn.getInputColumn());
                        VariableReferenceExpression outputColumn = getVariableReference(groupByColumn.getOutputColumn());
                        Selection pinotColumn = requireNonNull(context.getSelections().get(groupByInputColumn), "Group By column " + groupByInputColumn + " doesn't exist in input " + context.getSelections());

                        newSelections.put(outputColumn, new Selection(pinotColumn.getDefinition(), pinotColumn.getOrigin()));
                        groupByColumns.add(outputColumn);
                        groupByExists = true;
                        break;
                    }
                    case AGGREGATE: {
                        PushdownUtils.AggregationFunctionColumnNode aggr = (PushdownUtils.AggregationFunctionColumnNode) expr;
                        String pinotAggFunction = handleAggregationFunction(aggr.getCallExpression(), context.getSelections());
                        newSelections.put(getVariableReference(aggr.getOutputColumn()), new Selection(pinotAggFunction, DERIVED));
                        ++numAggregations;
                        break;
                    }
                    default:
                        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "unknown aggregation expression: " + expr.getExprType());
                }
            }

            // Handling non-aggregated group by
            if (groupByExists && numAggregations == 0) {
                VariableReferenceExpression hidden = new VariableReferenceExpression(UUID.randomUUID().toString(), BigintType.BIGINT);
                newSelections.put(hidden, new Selection("count(*)", DERIVED));
                hiddenColumnSet.add(hidden);
                numAggregations++;
            }
            return context.withAggregation(newSelections, groupByColumns, numAggregations, hiddenColumnSet);
        }

        @Override
        public PinotQueryGeneratorContext visitFilter(FilterNode node, PinotQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            PinotQueryGeneratorContext.Filter filter = new PinotQueryGeneratorContext.Filter(node.getPredicate(), node.getPredicate().accept(pinotFilterExpressionConverter, context.getSelections()).getDefinition(), false);
            return context.withFilter(filter).withOutputColumns(node.getOutputVariables());
        }

        @Override
        public PinotQueryGeneratorContext visitProject(ProjectNode node, PinotQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");

            LinkedHashMap<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();

            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                RowExpression expression = entry.getValue();
                PinotExpression pinotExpression = expression.accept(new PinotProjectExpressionConverter(typeManager, functionMetadataManager, session), context.getSelections());
                newSelections.put(
                        variable,
                        new Selection(pinotExpression.getDefinition(), pinotExpression.getOrigin()));
            }
            return context.withProject(newSelections);
        }

        @Override
        public PinotQueryGeneratorContext visitLimit(LimitNode node, PinotQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            return context.withLimit(node.getCount()).withOutputColumns(node.getOutputVariables());
        }

        @Override
        public PinotQueryGeneratorContext visitTopN(TopNNode node, PinotQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            return context.withTopN(PushdownUtils.getOrderingScheme(node), node.getCount()).withOutputColumns(node.getOutputVariables());
        }

        @Override
        public PinotQueryGeneratorContext visitTableScan(TableScanNode node, PinotQueryGeneratorContext context)
        {
            checkArgument(context == null, "Table scan node is expected to have no context as input");
            PinotTableHandle tableHandle = (PinotTableHandle) node.getTable().getConnectorHandle();
            checkState(!tableHandle.getSegmentPqlRealtime().isPresent(), "Expect to see no existing pql");
            checkState(!tableHandle.getSegmentPqlOffline().isPresent(), "Expect to see no existing pql");
            checkState(!tableHandle.getBrokerPql().isPresent(), "Expect to see no existing pql");
            LinkedHashMap<VariableReferenceExpression, Selection> selections = new LinkedHashMap<>();
            node.getAssignments().forEach((outputColumn, inputColumn) -> {
                PinotColumnHandle pinotColumn = (PinotColumnHandle) inputColumn;
                Preconditions.checkState(pinotColumn.getType().equals(PinotColumnHandle.PinotColumnType.REGULAR), "Unexpected pinot column handle that is not regular: %s", pinotColumn);
                selections.put(outputColumn, new Selection(pinotColumn.getColumnName(), TABLE_COLUMN));
            });
            return new PinotQueryGeneratorContext(node, selections, tableHandle.getTableName() + tableNameSuffix.orElse(""), timeBoundaryFilter);
        }
    }
}
