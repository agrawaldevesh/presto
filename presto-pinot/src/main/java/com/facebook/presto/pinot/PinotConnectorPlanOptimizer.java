package com.facebook.presto.pinot;

import com.facebook.presto.pinot.query.PinotFilterExpressionConverter;
import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.LogicalRowExpressions;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.ErrorType.USER_ERROR;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.extractConjuncts;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PinotConnectorPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(PinotConnectorPlanOptimizer.class);
    private final PinotQueryGenerator pinotQueryGenerator;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final LogicalRowExpressions logicalRowExpressions;
    private final PinotConfig pinotConfig;
    private final PinotConnection pinotConnection;
    private final String connectorId;

    @Inject
    public PinotConnectorPlanOptimizer(PinotQueryGenerator pinotQueryGenerator, TypeManager typeManager, FunctionMetadataManager functionMetadataManager, LogicalRowExpressions logicalRowExpressions, PinotConfig pinotConfig, PinotConnection pinotConnection, PinotConnectorId pinotConnectorId)
    {
        this.pinotQueryGenerator = requireNonNull(pinotQueryGenerator, "pinot query generator is null");
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.logicalRowExpressions = logicalRowExpressions;
        this.pinotConfig = pinotConfig;
        this.pinotConnection = pinotConnection;
        this.connectorId = pinotConnectorId.toString();
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        IdentityHashMap<TableScanNode, Void> scanNodes = maxSubplan.accept(new TableFindingVisitor(), null);
        Optional<TableScanNode> pinotTableScanNode = getOnlyPinotTable(scanNodes);
        if (pinotTableScanNode.isPresent()) {
            return maxSubplan.accept(new Visitor(pinotTableScanNode.get(), session, idAllocator), null).planNode;
        }
        else {
            return maxSubplan;
        }
    }

    private static Optional<PinotTableHandle> getPinotTableHandle(TableScanNode tableScanNode)
    {
        TableHandle table = tableScanNode.getTable();
        if (table != null) {
            ConnectorTableHandle connectorHandle = table.getConnectorHandle();
            if (connectorHandle instanceof PinotTableHandle) {
                return Optional.of((PinotTableHandle) connectorHandle);
            }
        }
        return Optional.empty();
    }

    private static Optional<TableScanNode> getOnlyPinotTable(IdentityHashMap<TableScanNode, Void> scanNodes)
    {
        if (scanNodes.size() == 1) {
            TableScanNode tableScanNode = scanNodes.keySet().iterator().next();
            if (getPinotTableHandle(tableScanNode).isPresent()) {
                return Optional.of(tableScanNode);
            }
        }
        return Optional.empty();
    }

    private class SegmentPqlGenerationState
    {
        private final Optional<String> offlineTimePredicate;
        private final Optional<String> onlineTimePredicate;

        public SegmentPqlGenerationState(String tableName)
        {
            Map<String, String> timeBoundary = pinotConnection.getTimeBoundary(tableName);

            if (timeBoundary.containsKey("timeColumnName") && timeBoundary.containsKey("timeColumnValue")) {
                String timeColumnName = timeBoundary.get("timeColumnName");
                String timeColumnValue = timeBoundary.get("timeColumnValue");

                offlineTimePredicate = Optional.of(format("%s < %s", timeColumnName, timeColumnValue));
                onlineTimePredicate = Optional.of(format("%s >= %s", timeColumnName, timeColumnValue));
            }
            else {
                onlineTimePredicate = Optional.empty();
                offlineTimePredicate = Optional.empty();
            }
        }

        public Optional<String> getOfflineTimePredicate()
        {
            return offlineTimePredicate;
        }

        public Optional<String> getOnlineTimePredicate()
        {
            return onlineTimePredicate;
        }
    }

    private static class PlanNodeAndContext
    {
        private final PlanNode planNode;
        private final Optional<PinotQueryGeneratorContext> pinotQueryGeneratorContext;

        public PlanNodeAndContext(PlanNode planNode, Optional<PinotQueryGeneratorContext> pinotQueryGeneratorContext)
        {
            this.planNode = planNode;
            this.pinotQueryGeneratorContext = pinotQueryGeneratorContext;
        }
    }

    private static PlanNode replaceChildren(PlanNode node, List<PlanNode> children)
    {
        for (int i = 0; i < node.getSources().size(); i++) {
            if (children.get(i) != node.getSources().get(i)) {
                return node.replaceChildren(children);
            }
        }
        return node;
    }

    private static class TableFindingVisitor
            extends PlanVisitor<IdentityHashMap<TableScanNode, Void>, Void>
    {
        @Override
        public IdentityHashMap<TableScanNode, Void> visitPlan(PlanNode node, Void context)
        {
            IdentityHashMap<TableScanNode, Void> ret = new IdentityHashMap<>();
            node.getSources().forEach(source -> ret.putAll(source.accept(this, context)));
            return ret;
        }

        @Override
        public IdentityHashMap<TableScanNode, Void> visitTableScan(TableScanNode node, Void context)
        {
            IdentityHashMap<TableScanNode, Void> ret = new IdentityHashMap<>();
            ret.put(node, null);
            return ret;
        }
    }

    // Single use visitor that needs the pinot table handle
    private class Visitor
            extends PlanVisitor<PlanNodeAndContext, Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final ConnectorSession session;
        private final TableScanNode tableScanNode;
        private final Optional<SegmentPqlGenerationState> segmentPqlGenerationState;
        private final IdentityHashMap<FilterNode, Void> filtersSplitUp = new IdentityHashMap<>();

        public Visitor(TableScanNode tableScanNode, ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.idAllocator = idAllocator;
            this.tableScanNode = tableScanNode;
            this.segmentPqlGenerationState = pinotConfig.isScanParallelismEnabled() ? Optional.of(new SegmentPqlGenerationState(getPinotTableHandle(this.tableScanNode).get().getTableName())) : Optional.empty();
        }

        private Optional<PlanNodeAndContext> tryCreatingNewScanNode(PlanNode plan)
        {
            Optional<PinotQueryGenerator.GeneratedPql> realtimeSegmentPql, offlineSegmentPql;
            PinotQueryGenerator.GeneratedPql brokerPql;
            PinotQueryGeneratorContext context;
            Optional<Boolean> scanParallelismEnabled;
            try {
                if (segmentPqlGenerationState.isPresent()) {
                    realtimeSegmentPql = Optional.of(pinotQueryGenerator.generateForSegmentSplits(plan, Optional.of("_REALTIME"), segmentPqlGenerationState.get().getOnlineTimePredicate(), session).getGeneratedPql());
                    offlineSegmentPql = Optional.of(pinotQueryGenerator.generateForSegmentSplits(plan, Optional.of("_OFFLINE"), segmentPqlGenerationState.get().getOfflineTimePredicate(), session).getGeneratedPql());
                }
                else {
                    realtimeSegmentPql = Optional.empty();
                    offlineSegmentPql = Optional.empty();
                }
                PinotQueryGenerator.PinotQueryGeneratorResult brokerRequest = pinotQueryGenerator.generateForSingleBrokerRequest(plan, session);
                brokerPql = brokerRequest.getGeneratedPql();
                context = brokerRequest.getContext();
                scanParallelismEnabled = Optional.of(ScanParallelismFinder.canParallelize(PinotSessionProperties.isScanParallelismEnabled(session), plan));
            }
            catch (Exception e) {
                log.debug(e, "Possibly benign error when pushing plan into scan node %s", plan);
                return Optional.empty();
            }
            TableHandle oldTableHandle = tableScanNode.getTable();
            LinkedHashMap<VariableReferenceExpression, PinotColumnHandle> assignments = context.getAssignments();
            PinotTableHandle pinotTableHandle = getPinotTableHandle(tableScanNode).get();
            Optional<Boolean> isQueryShort = PinotSessionProperties.getScanPipelineScanLimit(session).map(context::isQueryShort);
            TableHandle newTableHandle = new TableHandle(
                    oldTableHandle.getConnectorId(),
                    new PinotTableHandle(pinotTableHandle.getConnectorId(), pinotTableHandle.getSchemaName(), pinotTableHandle.getTableName(), scanParallelismEnabled, isQueryShort, realtimeSegmentPql, offlineSegmentPql, Optional.of(brokerPql)),
                    oldTableHandle.getTransaction(),
                    oldTableHandle.getLayout());
            return Optional.of(
                    new PlanNodeAndContext(
                            new TableScanNode(
                                    idAllocator.getNextId(),
                                    newTableHandle,
                                    ImmutableList.copyOf(assignments.keySet()),
                                    assignments.entrySet().stream().collect(toImmutableMap((e) -> e.getKey(), (e) -> (ColumnHandle) (e.getValue()))),
                                    tableScanNode.getCurrentConstraint(),
                                    tableScanNode.getEnforcedConstraint()),
                            Optional.of(context)));
        }

        @Override
        public PlanNodeAndContext visitPlan(PlanNode node, Void context)
        {
            Optional<PlanNodeAndContext> planNodeAndContext = tryCreatingNewScanNode(node);
            if (planNodeAndContext.isPresent()) {
                return planNodeAndContext.get();
            }
            else {
                PlanNode newNode = replaceChildren(node, node.getSources().stream().map(c -> c.accept(this, null).planNode).collect(toImmutableList()));
                return new PlanNodeAndContext(newNode, Optional.empty());
            }
        }

        @Override
        public PlanNodeAndContext visitFilter(FilterNode node, Void context)
        {
            if (filtersSplitUp.containsKey(node)) {
                return this.visitPlan(node, context);
            }
            PlanNodeAndContext childReturn = node.getSource().accept(this, context);
            PlanNodeAndContext returnWithoutPushingDown = new PlanNodeAndContext(replaceChildren(node, ImmutableList.of(childReturn.planNode)), Optional.empty());
            if (!childReturn.pinotQueryGeneratorContext.isPresent()) {
                return returnWithoutPushingDown;
            }
            LinkedHashMap<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> selections = childReturn.pinotQueryGeneratorContext.get().getSelections();
            List<RowExpression> pushable = new ArrayList<>();
            List<RowExpression> nonPushable = new ArrayList<>();
            PinotFilterExpressionConverter pinotFilterExpressionConverter = new PinotFilterExpressionConverter(typeManager, functionMetadataManager);
            for (RowExpression conjunct : extractConjuncts(node.getPredicate())) {
                try {
                    conjunct.accept(pinotFilterExpressionConverter, selections);
                    pushable.add(conjunct);
                }
                catch (PinotException pe) {
                    nonPushable.add(conjunct);
                }
            }
            if (pushable.isEmpty()) {
                return returnWithoutPushingDown;
            }
            FilterNode pushableFilter = new FilterNode(idAllocator.getNextId(), node.getSource(), logicalRowExpressions.combineConjuncts(pushable));
            Optional<FilterNode> nonPushableFilter = nonPushable.isEmpty() ? Optional.empty() : Optional.of(new FilterNode(idAllocator.getNextId(), pushableFilter, logicalRowExpressions.combineConjuncts(nonPushable)));

            filtersSplitUp.put(pushableFilter, null);
            if (nonPushableFilter.isPresent()) {
                FilterNode nonPushableFilterNode = nonPushableFilter.get();
                filtersSplitUp.put(nonPushableFilterNode, null);
                return this.visitFilter(nonPushableFilterNode, context);
            }
            else {
                return this.visitFilter(pushableFilter, context);
            }
        }
    }
}
