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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.protobuf.Any;
import io.substrait.proto.AdvancedExtension;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.ExtensionSingleRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.JoinRel;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.RelCommon.Hint.Stats;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SortRel;
import presto.substrait.PrestoStats.PrestoRelStats;

import java.util.Map;

import static com.facebook.presto.operator.scalar.MathFunctions.isNaN;

public class SubstraitStats
{
    /**
     * Get stats info from PlanNodeStatsEstimate, construct PrestoRelStats and add it as AdvancedExtension to RelCommon.Hint.Stats
     *
     * @param estimate estimate stats for PlanNode
     * @return Stats that will be added to proto.RelCommon.Hint
     */
    public static Stats toSubstraitStats(PlanNodeStatsEstimate estimate)
    {
        double rowCount = estimate.getOutputRowCount();
        double outputSizeInBytes = estimate.getOutputSizeInBytes();
        PrestoRelStats prestoStats = toPrestoRelStats(estimate);
        AdvancedExtension advancedExtension = AdvancedExtension.newBuilder().addOptimization(Any.pack(prestoStats)).build();

        return Stats.newBuilder().setRowCount(rowCount).setRecordSize(outputSizeInBytes).setAdvancedExtension(advancedExtension).build();
    }

    public static PrestoRelStats toPrestoRelStats(PlanNodeStatsEstimate nodeStats)
    {
        PrestoRelStats.Builder builder = PrestoRelStats.newBuilder();
        Map<VariableReferenceExpression, VariableStatsEstimate> allVarStats = nodeStats.getVariableStatistics();

        for (Map.Entry<VariableReferenceExpression, VariableStatsEstimate> entry : allVarStats.entrySet()) {
            VariableReferenceExpression var = entry.getKey();
            VariableStatsEstimate varStats = entry.getValue();
            String varName = var.getName();
            PrestoRelStats.ColumnStats.Builder col = PrestoRelStats.ColumnStats.newBuilder().setColumnName(varName);

            // Only set fields when they are known
            if (!isNaN(varStats.getLowValue())) {
                col.setLowValue(varStats.getLowValue());
            }
            if (!isNaN(varStats.getHighValue())) {
                col.setHighValue(varStats.getHighValue());
            }
            if (!isNaN(varStats.getNullsFraction())) {
                col.setNullsFraction(varStats.getNullsFraction());
            }
            if (!isNaN(varStats.getAverageRowSize())) {
                col.setAverageRowSize(varStats.getAverageRowSize());
            }
            if (!isNaN(varStats.getDistinctValuesCount())) {
                col.setDistinctValuesCount(varStats.getDistinctValuesCount());
            }

            builder.addColumns(col);
        }

        return builder.build();
    }

    static io.substrait.proto.Plan annotatePlanWithStats(io.substrait.proto.Plan protoPlan, Map<String, PlanNodeStatsEstimate> planNodeStatsMap)
    {
        io.substrait.proto.Plan.Builder planBuilder = protoPlan.toBuilder();
        // for each relation, check its type; for now, only handle NamedScan relation
        for (int i = 0; i < planBuilder.getRelationsCount(); i++) {
            PlanRel planRel = planBuilder.getRelations(i);
            RelRoot relRoot = planRel.getRoot();
            io.substrait.proto.Rel pathedRel = annotateRelWithStats(relRoot.getInput(), planNodeStatsMap);
            RelRoot patcheRelRoot = relRoot.toBuilder().setInput(pathedRel).build();
            PlanRel patchedPlanRel = planRel.toBuilder().setRoot(patcheRelRoot).build();
            planBuilder.setRelations(i, patchedPlanRel);
        }
        return planBuilder.build();
    }

    private static io.substrait.proto.Rel annotateRelWithStats(io.substrait.proto.Rel rel, Map<String, PlanNodeStatsEstimate> planNodeStatsMap)
    {
        io.substrait.proto.Rel.Builder relBuilder = rel.toBuilder();
        RelCommon common = getCommon(rel, planNodeStatsMap);

        switch (rel.getRelTypeCase()) {
            case READ: {
                relBuilder.setRead(rel.getRead().toBuilder().setCommon(common).build());
                break;
            }

            case EXTENSION_SINGLE: {
                ExtensionSingleRel es = rel.getExtensionSingle();
                Rel patchedInput = annotateRelWithStats(es.getInput(), planNodeStatsMap);
                relBuilder.setExtensionSingle(es.toBuilder().setInput(patchedInput).setCommon(common).build());
                break;
            }

            case JOIN: {
                JoinRel j = rel.getJoin();
                Rel patchedLeft = annotateRelWithStats(j.getLeft(), planNodeStatsMap);
                Rel patchedRight = annotateRelWithStats(j.getRight(), planNodeStatsMap);
                relBuilder.setJoin(j.toBuilder().setLeft(patchedLeft).setRight(patchedRight).setCommon(common).build());
                break;
            }

            case PROJECT: {
                ProjectRel p = rel.getProject();
                Rel patchedInput = annotateRelWithStats(p.getInput(), planNodeStatsMap);
                relBuilder.setProject(p.toBuilder().setInput(patchedInput).setCommon(common).build());
                break;
            }

            case FILTER: {
                FilterRel f = rel.getFilter();
                Rel patchedInput = annotateRelWithStats(f.getInput(), planNodeStatsMap);
                relBuilder.setFilter(f.toBuilder().setInput(patchedInput).setCommon(common).build());
                break;
            }

            case AGGREGATE: {
                AggregateRel a = rel.getAggregate();
                Rel patchedInput = annotateRelWithStats(a.getInput(), planNodeStatsMap);
                relBuilder.setAggregate(a.toBuilder().setInput(patchedInput).setCommon(common).build());
                break;
            }

            case SORT: {
                SortRel s = rel.getSort();
                Rel patchedInput = annotateRelWithStats(s.getInput(), planNodeStatsMap);
                relBuilder.setSort(s.toBuilder().setInput(patchedInput).setCommon(common).build());
                break;
            }

            default:
                break;
        }

        return relBuilder.build();
    }

    private static RelCommon getCommon(io.substrait.proto.Rel rel, Map<String, PlanNodeStatsEstimate> planNodeStatsMap)
    {
        RelCommon common = null;
        switch (rel.getRelTypeCase()) {
            case READ: {
                common = rel.getRead().getCommon();
                break;
            }
            case EXTENSION_SINGLE: {
                common = rel.getExtensionSingle().getCommon();
                break;
            }
            case JOIN: {
                common = rel.getJoin().getCommon();
                break;
            }
            case PROJECT: {
                common = rel.getProject().getCommon();
                break;
            }
            case FILTER: {
                common = rel.getFilter().getCommon();
                break;
            }
            case AGGREGATE: {
                common = rel.getAggregate().getCommon();
                break;
            }
            case SORT: {
                common = rel.getSort().getCommon();
                break;
            }
            default:
                break;
        }
        if (common == null) {
            return null;
        }
        RelCommon.Hint hint = common.getHint();
        if (!hint.getAlias().isEmpty()) {
            RelCommon.Hint.Stats stats = toSubstraitStats(planNodeStatsMap.get(hint.getAlias()));
            RelCommon.Hint patchedHint = hint.toBuilder().setStats(stats).build();
            return common.toBuilder().setHint(patchedHint).build();
        }
        return common;
    }
}
