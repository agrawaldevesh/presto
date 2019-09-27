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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TopNNode;

import static java.util.Objects.requireNonNull;

public class ScanParallelismFinder
        extends PlanVisitor<Boolean, Boolean>
{
    private ScanParallelismFinder()
    {
    }

    // go through the pipeline operations and see if we parallelize the scan
    public static boolean canParallelize(boolean canParallelize, PlanNode plan)
    {
        return requireNonNull(plan.accept(new ScanParallelismFinder(), canParallelize), "Can Parallize is indeterminate");
    }

    @Override
    public Boolean visitPlan(PlanNode node, Boolean canParallelize)
    {
        return canParallelize;
    }

    @Override
    public Boolean visitAggregation(AggregationNode node, Boolean canParallelize)
    {
        return node.getSource().accept(this, canParallelize) && node.getStep().isOutputPartial();
    }

    @Override
    public Boolean visitLimit(LimitNode node, Boolean canParallelize)
    {
        // we can only parallelize if the limit pushdown is split level (aka partial limit)
        return node.getSource().accept(this, canParallelize) && node.isPartial();
    }

    @Override
    public Boolean visitTopN(TopNNode topNNode, Boolean canParallelize)
    {
        return false;
    }
}
