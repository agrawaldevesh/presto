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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.FunctionType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.facebook.presto.execution.warnings.WarningCollector.NOOP;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.builder;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

// TODO(dagrawal): there is a lot of duplicate code here from the TranslsateExpressions to convert away the expressions into the row expressions proper to support the final aggregation pushdown use case
public class ApplyConnectorOptimization
        implements PlanOptimizer
{
    static final Set<Class<? extends PlanNode>> CONNECTOR_ACCESSIBLE_PLAN_NODES = ImmutableSet.of(
            FilterNode.class,
            TableScanNode.class,
            LimitNode.class,
            TopNNode.class,
            ValuesNode.class,
            ProjectNode.class,
            AggregationNode.class);

    // for a leaf node that does not belong to any connector (e.g., ValuesNode)
    private static final ConnectorId EMPTY_CONNECTOR_ID = new ConnectorId("$internal$" + ApplyConnectorOptimization.class + "_CONNECTOR");

    private final Supplier<Map<ConnectorId, Set<ConnectorPlanOptimizer>>> connectorOptimizersSupplier;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final boolean rowExpressionsExpected;

    public ApplyConnectorOptimization(Supplier<Map<ConnectorId, Set<ConnectorPlanOptimizer>>> connectorOptimizersSupplier, Metadata metadata, SqlParser sqlParser, boolean rowExpressionsExpected)
    {
        this.connectorOptimizersSupplier = requireNonNull(connectorOptimizersSupplier, "connectorOptimizersSupplier is null");
        this.metadata = requireNonNull(metadata, "metadata");
        this.sqlParser = requireNonNull(sqlParser, "metadata");
        this.rowExpressionsExpected = rowExpressionsExpected;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        Map<ConnectorId, Set<ConnectorPlanOptimizer>> connectorOptimizers = connectorOptimizersSupplier.get();
        if (connectorOptimizers.isEmpty()) {
            return plan;
        }

        // retrieve all the connectors
        ImmutableSet.Builder<ConnectorId> connectorIds = ImmutableSet.builder();
        getAllConnectorIds(plan, connectorIds);

        // for each connector, retrieve the set of subplans to optimize
        // TODO: what if a new connector is added by an existing one
        // There are cases (e.g., query federation) where a connector C1 needs to
        // create a UNION_ALL to federate data sources from both C1 and C2 (regardless of the classloader issue).
        // For such case, it is dangerous to re-calculate the "max closure" given the fixpoint property will be broken.
        // In order to preserve the fixpoint, we will "pretend" the newly added C2 table scan is part of C1's job to maintain.
        for (ConnectorId connectorId : connectorIds.build()) {
            Set<ConnectorPlanOptimizer> optimizers = connectorOptimizers.get(connectorId);
            if (optimizers == null) {
                continue;
            }

            ImmutableMap.Builder<PlanNode, ConnectorPlanNodeContext> contextMapBuilder = ImmutableMap.builder();
            buildConnectorPlanNodeContext(plan, null, contextMapBuilder);
            Map<PlanNode, ConnectorPlanNodeContext> contextMap = contextMapBuilder.build();

            // keep track of changed nodes; the keys are original nodes and the values are the new nodes
            Map<PlanNode, PlanNode> updates = new HashMap<>();

            // process connector optimizers
            for (PlanNode node : contextMap.keySet()) {
                // For a subtree with root `node` to be a max closure, the following conditions must hold:
                //    * The subtree with root `node` is a closure.
                //    * `node` has no parent, or the subtree with root as `node`'s parent is not a closure.
                ConnectorPlanNodeContext context = contextMap.get(node);
                if (!context.isClosure(connectorId) ||
                        !context.getParent().isPresent() ||
                        contextMap.get(context.getParent().get()).isClosure(connectorId)) {
                    continue;
                }

                PlanNode nodeAfterRowExpressionRewriting = rowExpressionsExpected ? node : SimplePlanRewriter.rewriteWith(new RowExpressionRewriter(variableAllocator, session, false), node, null);
                PlanNode newNode = nodeAfterRowExpressionRewriting;

                // the returned node is still a max closure (only if there is no new connector added, which does happen but ignored here)
                for (ConnectorPlanOptimizer optimizer : optimizers) {
                    newNode = optimizer.optimize(newNode, session.toConnectorSession(connectorId), variableAllocator, idAllocator);
                }

                if (nodeAfterRowExpressionRewriting != newNode) {
                    // the optimizer has allocated a new PlanNode
                    if (!rowExpressionsExpected) {
                        // Check that no row expressions have leaked in because subsequent optimizers are not able to handle this.
                        // TODO(sjames): fix this !
                        SimplePlanRewriter.rewriteWith(new RowExpressionRewriter(variableAllocator, session, true), newNode, null);
                    }
                    checkState(
                            containsAll(ImmutableSet.copyOf(newNode.getOutputVariables()), node.getOutputVariables()),
                            "the connector optimizer from %s returns a node that does not cover all output before optimization",
                            connectorId);
                    updates.put(node, newNode);
                }
            }
            // up to this point, we have a set of updated nodes; need to recursively update their parents

            // alter the plan with a bottom-up approach (but does not have to be strict bottom-up to guarantee the correctness of the algorithm)
            // use "original nodes" to keep track of the plan structure and "updates" to keep track of the new nodes
            Queue<PlanNode> originalNodes = new LinkedList<>(updates.keySet());
            while (!originalNodes.isEmpty()) {
                PlanNode originalNode = originalNodes.poll();

                if (!contextMap.get(originalNode).getParent().isPresent()) {
                    // originalNode must be the root; update the plan
                    plan = updates.get(originalNode);
                    continue;
                }

                PlanNode originalParent = contextMap.get(originalNode).getParent().get();

                // need to create a new parent given the child has changed; the new parent needs to point to the new child.
                // if a node has been updated, it will occur in `updates`; otherwise, just use the original node
                ImmutableList.Builder<PlanNode> newChildren = ImmutableList.builder();
                originalParent.getSources().forEach(child -> newChildren.add(updates.getOrDefault(child, child)));
                PlanNode newParent = originalParent.replaceChildren(newChildren.build());

                // mark the new parent as updated
                updates.put(originalParent, newParent);

                // enqueue the parent node in order to recursively update its ancestors
                originalNodes.add(originalParent);
            }
        }

        return plan;
    }

    private class RowExpressionRewriter
            extends SimplePlanRewriter<Void>
    {

        private final PlanVariableAllocator variableAllocator;
        private final Session session;
        private final boolean checkNoRowExpression;

        public RowExpressionRewriter(PlanVariableAllocator variableAllocator, Session session, boolean checkNoRowExpression)
        {
            this.variableAllocator = variableAllocator;
            this.session = session;
            this.checkNoRowExpression = checkNoRowExpression;
        }

        private RowExpression toRowExpression(Expression expression, Session session, Map<NodeRef<Expression>, Type> types)
        {
            return SqlToRowExpressionTranslator.translate(expression, types, ImmutableMap.of(), metadata.getFunctionManager(), metadata.getTypeManager(), session);
        }

        private Optional<Assignments> translateAssignments(Assignments assignments)
        {
            Assignments.Builder builder = Assignments.builder();
            boolean anyRewritten = false;
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignments.entrySet()) {
                RowExpression expression = entry.getValue();
                RowExpression rewritten;
                if (isExpression(expression)) {
                    rewritten = toRowExpression(
                            castToExpression(expression),
                            session,
                            analyze(castToExpression(expression), session, variableAllocator.getTypes()));
                    anyRewritten = true;
                }
                else {
                    checkState(!checkNoRowExpression, "Unexpected row expression in assignments: " + assignments.entrySet());
                    rewritten = expression;
                }
                builder.put(entry.getKey(), rewritten);
            }
            if (!anyRewritten) {
                return Optional.empty();
            }
            return Optional.of(builder.build());
        }

        private Map<NodeRef<Expression>, Type> analyze(Expression expression, Session session, TypeProvider typeProvider)
        {
            return getExpressionTypes(
                    session,
                    metadata,
                    sqlParser,
                    typeProvider,
                    expression,
                    emptyList(),
                    NOOP);
        }

        @Override
        public PlanNode visitProject(ProjectNode projectNode, RewriteContext<Void> context)
        {
            Assignments assignments = projectNode.getAssignments();
            Optional<Assignments> rewrittenAssignments = translateAssignments(assignments);
            PlanNode rewrittenSource = context.rewrite(projectNode.getSource());
            if (rewrittenAssignments.isPresent()) {
                return new ProjectNode(projectNode.getId(), rewrittenSource, rewrittenAssignments.get());
            }
            else {
                return replaceChildren(projectNode, ImmutableList.of(rewrittenSource));
            }
        }

        private RowExpression removeOriginalExpression(RowExpression expression)
        {
            if (isExpression(expression)) {
                return toRowExpression(
                        castToExpression(expression),
                        session,
                        analyze(castToExpression(expression), session, variableAllocator.getTypes()));
            }
            else if (checkNoRowExpression) {
                throw new IllegalStateException("Unexpected row expression " + expression);
            }
            return expression;
        }

        @Override
        public PlanNode visitFilter(FilterNode filterNode, RewriteContext<Void> context)
        {
            checkState(filterNode.getSource() != null);
            PlanNode rewrittenSource = context.rewrite(filterNode.getSource());
            RowExpression rewritten = removeOriginalExpression(filterNode.getPredicate());

            if (!filterNode.getPredicate().equals(rewritten)) {
                return new FilterNode(filterNode.getId(), rewrittenSource, rewritten);
            }
            else {
                return replaceChildren(filterNode, ImmutableList.of(rewrittenSource));
            }
        }

        private Map<NodeRef<Expression>, Type> analyzeAggregationExpressionTypes(AggregationNode.Aggregation aggregation, Session session, TypeProvider typeProvider)
        {
            List<LambdaExpression> lambdaExpressions = aggregation.getArguments().stream()
                    .filter(OriginalExpressionUtils::isExpression)
                    .map(OriginalExpressionUtils::castToExpression)
                    .filter(LambdaExpression.class::isInstance)
                    .map(LambdaExpression.class::cast)
                    .collect(toImmutableList());
            ImmutableMap.Builder<NodeRef<Expression>, Type> builder = ImmutableMap.<NodeRef<Expression>, Type>builder();
            if (!lambdaExpressions.isEmpty()) {
                List<FunctionType> functionTypes = metadata.getFunctionManager().getFunctionMetadata(aggregation.getFunctionHandle()).getArgumentTypes().stream()
                        .filter(typeSignature -> typeSignature.getBase().equals(FunctionType.NAME))
                        .map(typeSignature -> (FunctionType) (metadata.getTypeManager().getType(typeSignature)))
                        .collect(toImmutableList());
                InternalAggregationFunction internalAggregationFunction = metadata.getFunctionManager().getAggregateFunctionImplementation(aggregation.getFunctionHandle());
                List<Class> lambdaInterfaces = internalAggregationFunction.getLambdaInterfaces();
                verify(lambdaExpressions.size() == functionTypes.size());
                verify(lambdaExpressions.size() == lambdaInterfaces.size());

                for (int i = 0; i < lambdaExpressions.size(); i++) {
                    LambdaExpression lambdaExpression = lambdaExpressions.get(i);
                    FunctionType functionType = functionTypes.get(i);

                    // To compile lambda, LambdaDefinitionExpression needs to be generated from LambdaExpression,
                    // which requires the types of all sub-expressions.
                    //
                    // In project and filter expression compilation, ExpressionAnalyzer.getExpressionTypesFromInput
                    // is used to generate the types of all sub-expressions. (see visitScanFilterAndProject and visitFilter)
                    //
                    // This does not work here since the function call representation in final aggregation node
                    // is currently a hack: it takes intermediate type as input, and may not be a valid
                    // function call in Presto.
                    //
                    // TODO: Once the final aggregation function call representation is fixed,
                    // the same mechanism in project and filter expression should be used here.
                    verify(lambdaExpression.getArguments().size() == functionType.getArgumentTypes().size());
                    Map<NodeRef<Expression>, Type> lambdaArgumentExpressionTypes = new HashMap<>();
                    Map<String, Type> lambdaArgumentSymbolTypes = new HashMap<>();
                    for (int j = 0; j < lambdaExpression.getArguments().size(); j++) {
                        LambdaArgumentDeclaration argument = lambdaExpression.getArguments().get(j);
                        Type type = functionType.getArgumentTypes().get(j);
                        lambdaArgumentExpressionTypes.put(NodeRef.of(argument), type);
                        lambdaArgumentSymbolTypes.put(argument.getName().getValue(), type);
                    }
                    // the lambda expression itself
                    builder.put(NodeRef.of(lambdaExpression), functionType)
                            // expressions from lambda arguments
                            .putAll(lambdaArgumentExpressionTypes)
                            // expressions from lambda body
                            .putAll(getExpressionTypes(
                                    session,
                                    metadata,
                                    sqlParser,
                                    TypeProvider.copyOf(lambdaArgumentSymbolTypes),
                                    lambdaExpression.getBody(),
                                    emptyList(),
                                    NOOP));
                }
            }
            for (RowExpression argument : aggregation.getArguments()) {
                if (!isExpression(argument) || castToExpression(argument) instanceof LambdaExpression) {
                    continue;
                }
                builder.putAll(analyze(castToExpression(argument), session, typeProvider));
            }
            if (aggregation.getFilter().isPresent() && isExpression(aggregation.getFilter().get())) {
                builder.putAll(analyze(castToExpression(aggregation.getFilter().get()), session, typeProvider));
            }
            return builder.build();
        }

        @VisibleForTesting
        public AggregationNode.Aggregation translateAggregation(AggregationNode.Aggregation aggregation, Session session, TypeProvider typeProvider)
        {
            Map<NodeRef<Expression>, Type> types = analyzeAggregationExpressionTypes(aggregation, session, typeProvider);

            return new AggregationNode.Aggregation(
                    new CallExpression(
                            aggregation.getCall().getDisplayName(),
                            aggregation.getCall().getFunctionHandle(),
                            aggregation.getCall().getType(),
                            aggregation.getArguments().stream().map(argument -> removeOriginalExpression(argument)).collect(toImmutableList())),
                    aggregation.getFilter().map(filter -> removeOriginalExpression(filter)),
                    aggregation.getOrderBy(),
                    aggregation.isDistinct(),
                    aggregation.getMask());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            checkState(node.getSource() != null);
            PlanNode rewrittenSource = context.rewrite(node.getSource());

            boolean changed = false;
            ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> rewrittenAggregation = builder();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                AggregationNode.Aggregation rewritten = translateAggregation(entry.getValue(), session, variableAllocator.getTypes());
                rewrittenAggregation.put(entry.getKey(), rewritten);
                if (!rewritten.equals(entry.getValue())) {
                    changed = true;
                }
            }

            if (changed) {
                return new AggregationNode(
                        node.getId(),
                        rewrittenSource,
                        rewrittenAggregation.build(),
                        node.getGroupingSets(),
                        node.getPreGroupedVariables(),
                        node.getStep(),
                        node.getHashVariable(),
                        node.getGroupIdVariable());
            }
            return replaceChildren(node, ImmutableList.of(rewrittenSource));
        }
    }

    private static void getAllConnectorIds(PlanNode node, ImmutableSet.Builder<ConnectorId> builder)
    {
        if (node.getSources().isEmpty()) {
            if (node instanceof TableScanNode) {
                builder.add(((TableScanNode) node).getTable().getConnectorId());
            }
            else {
                builder.add(EMPTY_CONNECTOR_ID);
            }
            return;
        }

        for (PlanNode child : node.getSources()) {
            getAllConnectorIds(child, builder);
        }
    }

    private static ConnectorPlanNodeContext buildConnectorPlanNodeContext(
            PlanNode node,
            PlanNode parent,
            ImmutableMap.Builder<PlanNode, ConnectorPlanNodeContext> contextBuilder)
    {
        Set<ConnectorId> connectorIds;
        Set<Class<? extends PlanNode>> planNodeTypes;

        if (node.getSources().isEmpty()) {
            if (node instanceof TableScanNode) {
                connectorIds = ImmutableSet.of(((TableScanNode) node).getTable().getConnectorId());
                planNodeTypes = ImmutableSet.of(TableScanNode.class);
            }
            else {
                connectorIds = ImmutableSet.of(EMPTY_CONNECTOR_ID);
                planNodeTypes = ImmutableSet.of(node.getClass());
            }
        }
        else {
            connectorIds = new HashSet<>();
            planNodeTypes = new HashSet<>();

            for (PlanNode child : node.getSources()) {
                ConnectorPlanNodeContext childContext = buildConnectorPlanNodeContext(child, node, contextBuilder);
                connectorIds.addAll(childContext.getReachableConnectors());
                planNodeTypes.addAll(childContext.getReachablePlanNodeTypes());
            }
            planNodeTypes.add(node.getClass());
        }

        ConnectorPlanNodeContext connectorPlanNodeContext = new ConnectorPlanNodeContext(
                parent,
                connectorIds,
                planNodeTypes);

        contextBuilder.put(node, connectorPlanNodeContext);
        return connectorPlanNodeContext;
    }

    /**
     * Extra information needed for a plan node
     */
    private static final class ConnectorPlanNodeContext
    {
        private final PlanNode parent;
        private final Set<ConnectorId> reachableConnectors;
        private final Set<Class<? extends PlanNode>> reachablePlanNodeTypes;

        ConnectorPlanNodeContext(PlanNode parent, Set<ConnectorId> reachableConnectors, Set<Class<? extends PlanNode>> reachablePlanNodeTypes)
        {
            this.parent = parent;
            this.reachableConnectors = requireNonNull(reachableConnectors, "reachableConnectors is null");
            this.reachablePlanNodeTypes = requireNonNull(reachablePlanNodeTypes, "reachablePlanNodeTypes is null");
            checkArgument(!reachableConnectors.isEmpty(), "encountered a PlanNode that reaches no connector");
            checkArgument(!reachablePlanNodeTypes.isEmpty(), "encountered a PlanNode that reaches no plan node");
        }

        Optional<PlanNode> getParent()
        {
            return Optional.ofNullable(parent);
        }

        public Set<ConnectorId> getReachableConnectors()
        {
            return reachableConnectors;
        }

        public Set<Class<? extends PlanNode>> getReachablePlanNodeTypes()
        {
            return reachablePlanNodeTypes;
        }

        boolean isClosure(ConnectorId connectorId)
        {
            // check if all children can reach the only connector
            if (reachableConnectors.size() != 1 || !reachableConnectors.contains(connectorId)) {
                return false;
            }

            // check if all children are accessible by connectors
            return containsAll(CONNECTOR_ACCESSIBLE_PLAN_NODES, reachablePlanNodeTypes);
        }
    }

    private static <T> boolean containsAll(Set<T> container, Collection<T> test)
    {
        for (T element : test) {
            if (!container.contains(element)) {
                return false;
            }
        }
        return true;
    }
}
