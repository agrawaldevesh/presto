package com.facebook.presto.pinot;

import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

public class PushdownUtils
{

    private PushdownUtils() {}

    public enum ExprType
    {
        GROUP_BY,
        AGGREGATE,
    }

    /**
     * Group by field description
     */
    public static class GroupByColumnNode
            extends AggregationColumnNode
    {
        private final VariableReferenceExpression inputColumn;

        public GroupByColumnNode(VariableReferenceExpression inputColumn, VariableReferenceExpression output)
        {
            super(ExprType.GROUP_BY, output);
            this.inputColumn = inputColumn;
        }

        public VariableReferenceExpression getInputColumn()
        {
            return inputColumn;
        }

        @Override
        public String toString()
        {
            return inputColumn.toString();
        }
    }

    /**
     * Agg function description.
     */
    public static class AggregationFunctionColumnNode
            extends AggregationColumnNode
    {
        private final CallExpression callExpression;

        public AggregationFunctionColumnNode(VariableReferenceExpression output, CallExpression callExpression)
        {
            super(ExprType.AGGREGATE, output);
            this.callExpression = callExpression;
        }

        public CallExpression getCallExpression()
        {
            return callExpression;
        }

        @Override
        public String toString()
        {
            return callExpression.toString();
        }
    }

    public static abstract class AggregationColumnNode
    {
        private final ExprType exprType;
        private final VariableReferenceExpression outputColumn;

        public AggregationColumnNode(ExprType exprType, VariableReferenceExpression outputColumn)
        {
            this.exprType = exprType;
            this.outputColumn = outputColumn;
        }

        public VariableReferenceExpression getOutputColumn()
        {
            return outputColumn;
        }

        public ExprType getExprType()
        {
            return exprType;
        }
    }

    public static Optional<List<AggregationColumnNode>> computeAggregationNodes(AggregationNode aggregationNode)
    {
        int groupByKeyIndex = 0;
        boolean supported = true;
        ImmutableList.Builder<AggregationColumnNode> nodeBuilder = ImmutableList.builder();
        for (int fieldId = 0; fieldId < aggregationNode.getOutputVariables().size() && supported; fieldId++) {
            VariableReferenceExpression outputColumn = aggregationNode.getOutputVariables().get(fieldId);
            AggregationNode.Aggregation agg = aggregationNode.getAggregations().get(outputColumn);

            if (agg != null) {
                CallExpression aggFunction = agg.getCall();
                if (aggFunction.getArguments().stream().allMatch(e -> (e instanceof VariableReferenceExpression)) && !agg.getFilter().isPresent() && !agg.isDistinct() && !agg.getOrderBy().isPresent() && !agg.getMask().isPresent()) {
                    nodeBuilder.add(new AggregationFunctionColumnNode(outputColumn, aggFunction));
                }
                else {
                    supported = false;
                }
            }
            else {
                // group by output
                VariableReferenceExpression inputColumn = aggregationNode.getGroupingKeys().get(groupByKeyIndex);
                nodeBuilder.add(new GroupByColumnNode(inputColumn, outputColumn));
                groupByKeyIndex++;
            }
        }
        return supported ? Optional.of(nodeBuilder.build()) : Optional.empty();
    }

    public static LinkedHashMap<VariableReferenceExpression, SortOrder> getOrderingScheme(TopNNode topNNode)
    {
        LinkedHashMap<VariableReferenceExpression, SortOrder> p = new LinkedHashMap<>();
        topNNode.getOrderingScheme().getOrderingsMap().forEach((v, s) -> p.put(v, s));
        return p;
    }
}
