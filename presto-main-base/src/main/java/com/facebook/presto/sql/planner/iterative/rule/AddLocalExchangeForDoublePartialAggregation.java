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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isDoublePartialAggregationEnabled;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static java.util.Objects.requireNonNull;

/**
 * Experimental, NOT cost-based. Gated solely by the {@code experimental.double_partial_aggregation}
 * session property.
 * <p>
 * Transforms
 * <pre>
 *   - RemoteExchange
 *     - [ Projection ]
 *       - Partial Aggregation
 * </pre>
 * to
 * <pre>
 *   - RemoteExchange
 *     - [ Projection ]
 *       - Aggregation (INTERMEDIATE)
 *         - LocalExchange (hash-partitioned on the grouping keys)
 *           - Partial Aggregation
 * </pre>
 * <p>
 * Rationale: a partial aggregation running independently on every driver only ever sees a
 * random slice of the full key space, so for high-cardinality grouping keys it barely reduces
 * row count before the shuffle. Redistributing rows locally by grouping key first, then running
 * a second aggregation stage, lets each local partition see the full local key domain instead of
 * a random sample of it.
 */
public class AddLocalExchangeForDoublePartialAggregation
        implements Rule<AggregationNode>
{
    // Marks the inner PARTIAL aggregation created by this rule so its pattern does not match it
    // again, which would otherwise cause the rule to re-apply to its own output forever.
    private static final int SYNTHETIC_AGGREGATION_ID = Integer.MIN_VALUE;

    private static final Pattern<AggregationNode> PATTERN = typeOf(AggregationNode.class)
            .matching(node -> node.getStep() == PARTIAL
                    && !node.getGroupingKeys().isEmpty()
                    && !node.getAggregationId().equals(Optional.of(SYNTHETIC_AGGREGATION_ID)));

    private final FunctionAndTypeManager functionAndTypeManager;

    public AddLocalExchangeForDoublePartialAggregation(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isDoublePartialAggregationEnabled(session);
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Map<VariableReferenceExpression, AggregationNode.Aggregation> innerAggregations = new LinkedHashMap<>();
        Map<VariableReferenceExpression, AggregationNode.Aggregation> outerAggregations = new LinkedHashMap<>();

        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
            AggregationNode.Aggregation originalAggregation = entry.getValue();
            FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
            String functionName = functionAndTypeManager.getFunctionMetadata(functionHandle).getName().getObjectName();
            AggregationFunctionImplementation function = functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);

            // The inner aggregation is an exact copy of the original partial aggregation: it
            // still consumes raw input and produces intermediate state.
            VariableReferenceExpression innerVariable = context.getVariableAllocator().newVariable(
                    originalAggregation.getCall().getSourceLocation(), functionName, function.getIntermediateType());
            innerAggregations.put(innerVariable, originalAggregation);

            // The outer aggregation merges intermediate state produced by the (now locally
            // repartitioned) inner aggregation, so it consumes and produces intermediate state,
            // and drops filter/distinct/mask, which only apply when consuming raw input.
            outerAggregations.put(entry.getKey(), new AggregationNode.Aggregation(
                    new CallExpression(
                            originalAggregation.getCall().getSourceLocation(),
                            functionName,
                            functionHandle,
                            function.getIntermediateType(),
                            ImmutableList.of(innerVariable)),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty()));
        }

        PlanNode innerAggregation = new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getSource(),
                innerAggregations,
                node.getGroupingSets(),
                node.getPreGroupedVariables(),
                PARTIAL,
                node.getHashVariable(),
                node.getGroupIdVariable(),
                Optional.of(SYNTHETIC_AGGREGATION_ID));

        PlanNode localExchange = partitionedExchange(
                context.getIdAllocator().getNextId(),
                LOCAL,
                innerAggregation,
                new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, node.getGroupingKeys()),
                        innerAggregation.getOutputVariables()));

        PlanNode outerAggregation = new AggregationNode(
                node.getSourceLocation(),
                node.getId(),
                localExchange,
                outerAggregations,
                node.getGroupingSets(),
                ImmutableList.of(),
                AggregationNode.Step.INTERMEDIATE,
                node.getHashVariable(),
                node.getGroupIdVariable(),
                node.getAggregationId());

        return Result.ofPlanNode(outerAggregation);
    }
}
