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
import com.facebook.presto.pinot.PinotErrorCode;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Encapsulates the components needed to construct a PQL query and provides methods to update the current context with new operations.
 */
public class PinotQueryGeneratorContext
{
    // Fields defining the query
    // order map that maps the column definition in terms of input relation column(s)
    private final LinkedHashMap<VariableReferenceExpression, Selection> selections;
    private final LinkedHashSet<VariableReferenceExpression> groupByColumns;
    private final LinkedHashMap<VariableReferenceExpression, SortOrder> topNColumnOrderingMap;
    private final HashSet<VariableReferenceExpression> hiddenColumnSet;
    private final String from;
    private final Filter filter;
    private final Optional<String> timeBoundaryFilter;
    private final Optional<Long> limit;
    private final int numAggregations;

    public boolean isQueryShort(long nonAggregateRowLimit)
    {
        return hasAggregation() || limit.orElse(Long.MAX_VALUE) < nonAggregateRowLimit;
    }

    public static class Filter
    {
        private final RowExpression predicate;
        private final String filter;
        private final boolean partial;

        public Filter(RowExpression predicate, String filter, boolean partial)
        {
            this.predicate = predicate;
            this.filter = filter;
            this.partial = partial;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("predicate", predicate)
                    .add("filter", filter)
                    .add("partial", partial)
                    .toString();
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("selections", selections)
                .add("groupByColumns", groupByColumns)
                .add("hiddenColumnSet", hiddenColumnSet)
                .add("from", from)
                .add("filter", filter)
                .add("timeBoundaryFilter", timeBoundaryFilter)
                .add("limit", limit)
                .add("numAggregations", numAggregations)
                .toString();
    }

    PinotQueryGeneratorContext(TableScanNode tableScanNode, LinkedHashMap<VariableReferenceExpression, Selection> selections, String from, Optional<String> timeBoundaryFilter)
    {
        this(selections, from, null, timeBoundaryFilter, 0, new LinkedHashSet<>(), new LinkedHashMap<>(), Optional.empty(), new HashSet<>());
    }

    private PinotQueryGeneratorContext(LinkedHashMap<VariableReferenceExpression, Selection> selections, String from, Filter filter, Optional<String> timeBoundaryFilter,
            int numAggregations, LinkedHashSet<VariableReferenceExpression> groupByColumns, LinkedHashMap<VariableReferenceExpression, SortOrder> topNColumnOrderingMap, Optional<Long> limit, HashSet<VariableReferenceExpression> hiddenColumnSet)
    {
        this.selections = requireNonNull(selections, "selections can't be null");
        this.from = requireNonNull(from, "source can't be null");
        this.numAggregations = numAggregations;
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available");
        this.topNColumnOrderingMap = requireNonNull(topNColumnOrderingMap, "topNColumnOrderingMap can't be null. It could be empty if not available");
        this.filter = filter;
        this.timeBoundaryFilter = requireNonNull(timeBoundaryFilter, "timeBoundaryFilter can't be null");
        this.limit = limit;
        this.hiddenColumnSet = hiddenColumnSet;
    }

    /**
     * Apply the given filter to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withFilter(Filter filter)
    {
        checkState(!hasFilter(), "There already exists a filter. Pinot doesn't support filters at multiple levels");
        checkState(!hasAggregation(), "Pinot doesn't support filtering the results of aggregation");
        checkState(!hasLimit(), "Pinot doesn't support filtering on top of the limit");
        return new PinotQueryGeneratorContext(selections, from, filter, timeBoundaryFilter, numAggregations,
                groupByColumns, topNColumnOrderingMap, limit, hiddenColumnSet);
    }

    /**
     * Apply the aggregation to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withAggregation(LinkedHashMap<VariableReferenceExpression, Selection> newSelections,
            LinkedHashSet<VariableReferenceExpression> groupByColumns, int numAggregations, HashSet<VariableReferenceExpression> hiddenColumnSet)
    {
        // there is only one aggregation supported.
        checkState(!hasAggregation(), "Pinot doesn't support aggregation on top of the aggregated data");
        checkState(!hasPartialFilter(), "No aggregation on top of partial filter");
        checkState(!hasLimit(), "Pinot doesn't support aggregation on top of the limit");
        return new PinotQueryGeneratorContext(newSelections, from, filter, timeBoundaryFilter, this.numAggregations +
                numAggregations, groupByColumns, topNColumnOrderingMap, limit, hiddenColumnSet);
    }

    /**
     * Apply new selections/project to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withProject(LinkedHashMap<VariableReferenceExpression, Selection> newSelections)
    {
        checkState(groupByColumns.isEmpty(), "Pinot doesn't yet support new selections on top of the grouped by data");
        return new PinotQueryGeneratorContext(newSelections, from, filter, timeBoundaryFilter, numAggregations,
                groupByColumns, topNColumnOrderingMap, limit, hiddenColumnSet);
    }

    /**
     * Apply limit to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withLimit(long limit)
    {
        checkState(!hasLimit(), "Limit already exists. Pinot doesn't support limit on top of another limit");
        checkState(!hasPartialFilter(), "No limit on top of partial filter");
        return new PinotQueryGeneratorContext(selections, from, filter, timeBoundaryFilter, numAggregations, groupByColumns, topNColumnOrderingMap, Optional.of(limit), hiddenColumnSet);
    }

    private boolean hasPartialFilter()
    {
        return filter != null && filter.partial;
    }

    /**
     * Apply order by to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withTopN(LinkedHashMap<VariableReferenceExpression, SortOrder> orderByColumnOrderingMap, long limit)
    {
        checkState(!hasLimit(), "Limit already exists. Pinot doesn't support order by limit on top of another limit");
        checkState(!hasAggregation(), "Pinot doesn't support ordering on top of the aggregated data");
        checkState(!hasPartialFilter(), "No topn on top of partial filter");
        return new PinotQueryGeneratorContext(selections, from, filter, timeBoundaryFilter, numAggregations, groupByColumns, orderByColumnOrderingMap, Optional.of(limit), hiddenColumnSet);
    }

    private boolean hasFilter()
    {
        return filter != null;
    }

    private boolean hasLimit()
    {
        return limit.isPresent();
    }

    private boolean hasAggregation()
    {
        return numAggregations > 0;
    }

    private boolean hasOrderBy()
    {
        return topNColumnOrderingMap.size() > 0;
    }

    public LinkedHashMap<VariableReferenceExpression, Selection> getSelections()
    {
        return selections;
    }

    public HashSet<VariableReferenceExpression> getHiddenColumnSet()
    {
        return hiddenColumnSet;
    }

    /**
     * Convert the current context to a PQL
     */
    public PinotQueryGenerator.GeneratedPql toQuery(PinotConfig pinotConfig)
    {
        if (!pinotConfig.isAllowMultipleAggregations() && numAggregations > 1 && !groupByColumns.isEmpty()) {
            throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Multiple aggregates in the presence of group by is forbidden");
        }

        if (hasLimit() && numAggregations > 1 && !groupByColumns.isEmpty()) {
            throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Multiple aggregates in the presence of group by and limit is forbidden");
        }

        String exprs = selections.entrySet().stream()
                .filter(s -> !groupByColumns.contains(s.getKey())) // remove the group by columns from the query as Pinot barfs if the group by column is an expression
                .map(s -> s.getValue().getDefinition())
                .collect(Collectors.joining(", "));

        String query = "SELECT " + exprs + " FROM " + from;
        if (filter != null) {
            String finalFilter = filter.filter;
            if (timeBoundaryFilter.isPresent()) {
                // this is hack!!!. Ideally we want to clone the scan pipeline and create/update the filter in the scan pipeline to contain this filter and
                // at the same time add the timecolumn to scan so that the query generator doesn't fail when it looks up the time column in scan output columns
                finalFilter = timeBoundaryFilter.get() + " AND " + filter + "";
            }
            query = query + " WHERE " + finalFilter;
        }

        if (!groupByColumns.isEmpty()) {
            String groupByExpr = groupByColumns.stream().map(x -> selections.get(x).getDefinition()).collect(Collectors.joining(", "));
            query = query + " GROUP BY " + groupByExpr;
        }

        if (hasOrderBy()) {
            String orderByExprs = topNColumnOrderingMap.entrySet().stream().map(entry -> selections.get(entry.getKey()).getDefinition() + (entry.getValue().isAscending() ? "" : " DESC")).collect(Collectors.joining(", "));
            query = query + " ORDER BY " + orderByExprs;
        }
        // Rules for limit:
        // - If its a selection query:
        //      + given limit or configured limit
        // - Else if has group by:
        //      + ensure that only one aggregation
        //      + default limit or configured top limit
        // - Fail if limit is invalid

        String limitKeyWord = "";
        long limitLong = -1;

        if (!hasAggregation()) {
            long defaultLimit = pinotConfig.getLimitLarge();
            limitLong = limit.orElse(defaultLimit);
            limitKeyWord = "LIMIT";
        }
        else if (!groupByColumns.isEmpty()) {
            limitKeyWord = "TOP";
            if (limit.isPresent()) {
                if (numAggregations > 1) {
                    throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.of(query),
                            String.format("Pinot has weird semantics with group by and multiple aggregation functions and limits"));
                }
                else {
                    limitLong = limit.get();
                }
            }
            else {
                limitLong = pinotConfig.getTopNLarge();
            }
        }

        if (!limitKeyWord.isEmpty()) {
            if (limitLong <= 0 || limitLong > Integer.MAX_VALUE) {
                throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Limit " + limitLong + " not supported: Limit is not being pushed down");
            }
            query += " " + limitKeyWord + " " + (int) limitLong;
        }

        List<PinotColumnHandle> columnHandles = ImmutableList.copyOf(getAssignments().values());
        return new PinotQueryGenerator.GeneratedPql(from, query, getIndicesMappingFromPinotSchemaToPrestoSchema(query, columnHandles), groupByColumns.size());
    }

    private List<Integer> getIndicesMappingFromPinotSchemaToPrestoSchema(String query, List<PinotColumnHandle> handles)
    {
        LinkedHashMap<VariableReferenceExpression, Selection> expressionsInPinotOrder = new LinkedHashMap<>();
        for (VariableReferenceExpression groupByColumn : groupByColumns) {
            Selection groupByColumnDefinition = selections.get(groupByColumn);
            if (groupByColumnDefinition == null) {
                throw new IllegalStateException(format("Group By column (%s) definition not found in input selections: ",
                        groupByColumn, Joiner.on(",").withKeyValueSeparator(":").join(selections)));
            }
            expressionsInPinotOrder.put(groupByColumn, groupByColumnDefinition);
        }
        expressionsInPinotOrder.putAll(selections);

        checkState(handles.size() == expressionsInPinotOrder.keySet().stream().filter(key -> !hiddenColumnSet.contains
                        (key)).count(), "Expected returned expressions %s to match selections %s",
                Joiner.on(",").withKeyValueSeparator(":").join(expressionsInPinotOrder), Joiner.on(",").join(handles));
        Map<VariableReferenceExpression, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < handles.size(); ++i) {
            PinotColumnHandle columnHandle = handles.get(i);
            VariableReferenceExpression columnName = new VariableReferenceExpression(columnHandle.getColumnName().toLowerCase(ENGLISH), columnHandle.getDataType());
            Integer prev = nameToIndex.put(columnName, i);
            if (prev != null) {
                throw new PinotException(PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION, Optional.of(query), format("Expected Pinot column handle %s to occur only once, but we have: %s", columnName, Joiner.on(",").join(handles)));
            }
        }
        ImmutableList.Builder<Integer> outputIndices = ImmutableList.builder();
        for (Map.Entry<VariableReferenceExpression, Selection> expression : expressionsInPinotOrder.entrySet()) {
            Integer index = nameToIndex.get(expression.getKey());
            if (hiddenColumnSet.contains(expression.getKey())) {
                index = -1; // negative output index means to skip this value returned by pinot at query time
            }
            if (index == null) {
                throw new PinotException(PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION, Optional.of(query), format("Expected to find a Pinot column handle for the expression %s, but we have %s",
                        expression, Joiner.on(",").withKeyValueSeparator(":").join(nameToIndex)));
            }
            outputIndices.add(index);
        }
        return outputIndices.build();
    }

    public LinkedHashMap<VariableReferenceExpression, PinotColumnHandle> getAssignments()
    {
        LinkedHashMap<VariableReferenceExpression, PinotColumnHandle> ret = new LinkedHashMap<>();
        selections.entrySet().stream().filter(e -> !hiddenColumnSet.contains(e.getKey())).forEach(entry -> {
            VariableReferenceExpression variable = entry.getKey();
            Selection selection = entry.getValue();
            PinotColumnHandle handle = selection.getOrigin() == Origin.TABLE_COLUMN ? new PinotColumnHandle(selection.getDefinition(), variable.getType(), PinotColumnHandle.PinotColumnType.REGULAR) : new PinotColumnHandle(variable, PinotColumnHandle.PinotColumnType.DERIVED);
            ret.put(variable, handle);
        });
        return ret;
    }

    public PinotQueryGeneratorContext withOutputColumns(List<VariableReferenceExpression> outputColumns)
    {
        LinkedHashMap<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();
        outputColumns.forEach(o -> newSelections.put(o, requireNonNull(selections.get(o), String.format("Cannot find the selection %s in the original context %s", o, this))));

        // Hidden columns flow as is from the previous
        selections.entrySet().stream().filter(e -> hiddenColumnSet.contains(e.getKey())).forEach(e -> newSelections.put(e.getKey(), e.getValue()));
        return new PinotQueryGeneratorContext(newSelections, from, filter, timeBoundaryFilter, numAggregations, groupByColumns, topNColumnOrderingMap, limit, hiddenColumnSet);
    }

    /**
     * Where is the selection/projection originated from
     */
    enum Origin
    {
        TABLE_COLUMN, // refers to direct column in table
        DERIVED, // expression is derived from one or more input columns or a combination of input columns and literals
        LITERAL, // derived from literal
    }

    // Projected/selected column definition in query
    public static class Selection
    {
        private final String definition;
        private final Origin origin;

        Selection(String definition, Origin origin)
        {
            this.definition = definition;
            this.origin = origin;
        }

        public String getDefinition()
        {
            return definition;
        }

        public Origin getOrigin()
        {
            return origin;
        }

        @Override
        public String toString()
        {
            return definition;
        }
    }
}
