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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.ProjectNodeUtils;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.relation.DomainTranslator.BASIC_COLUMN_EXTRACTOR;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * Push down aggregation's mask based on filter predicate.
 * <p>
 * This rule transforms plans with a FilterNode above an AggregationNode.
 * The AggregationNode must be grouped and contain a single aggregation
 * assignment with {@code count()}
 * <p>
 * If the filter predicate is {@code false} for the aggregation's result value {@code 0},
 * then the aggregation's mask can be removed from the aggregation, and
 * applied as a filter below the AggregationNode. After such transformation,
 * any group such that no rows of that group pass the filter, is removed
 * by the pushed down FilterNode, and so it is not processed by the
 * AggregationNode. Before the transformation, the group would be processed
 * by the AggregationNode, and return {@code 0}, which would then be filtered out
 * by the root FilterNode.
 * <p>
 * After the mask pushdown, it is checked whether the root FilterNode is
 * still needed, based on the fact that the aggregation never returns {@code 0}.
 * <p>
 * Transforms:
 * <pre> {@code
 * - filter (count > 0 AND predicate)
 *     - aggregation
 *       group by a
 *       count <- count() mask: m
 *         - source (a, m)
 *       }
 * </pre>
 * into:
 * <pre> {@code
 * - filter (predicate)
 *     - aggregation
 *       group by a
 *       count <- count()
 *         - filter (m)
 *             - source (a, m)
 *       }
 * </pre>
 *
 */
public class PushFilterThroughCountAggregation
{
    private final Metadata metadata;
    private final FunctionAndTypeManager functionAndTypeManager;

    public PushFilterThroughCountAggregation(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new PushFilterThroughCountAggregationWithoutProject(metadata),
                new PushFilterThroughCountAggregationWithProject(metadata));
    }

    public static final class PushFilterThroughCountAggregationWithoutProject
            implements Rule<FilterNode>
    {
        private static final Capture<AggregationNode> AGGREGATION = newCapture();

        private final Metadata metadata;
        private final FunctionResolution functionResolution;
        private final Pattern<FilterNode> pattern;

        public PushFilterThroughCountAggregationWithoutProject(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
            this.pattern = filter()
                    .with(source().matching(aggregation()
                            .matching(node -> isGroupedCountWithMask(node, functionResolution))
                            .capturedAs(AGGREGATION)));
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return pattern;
        }

        @Override
        public Result apply(FilterNode node, Captures captures, Context context)
        {
            return pushFilter(node, captures.get(AGGREGATION), Optional.empty(), metadata, context);
        }
    }

    public static final class PushFilterThroughCountAggregationWithProject
            implements Rule<FilterNode>
    {
        private static final Capture<ProjectNode> PROJECT = newCapture();
        private static final Capture<AggregationNode> AGGREGATION = newCapture();

        private final Metadata metadata;
        private final FunctionResolution functionResolution;
        private final Pattern<FilterNode> pattern;

        public PushFilterThroughCountAggregationWithProject(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
            this.pattern = filter()
                    .with(source().matching(project()
                            .matching(ProjectNodeUtils::isIdentity)
                            .capturedAs(PROJECT)
                            .with(source().matching(aggregation()
                                    .matching(node -> isGroupedCountWithMask(node, functionResolution))
                                    .capturedAs(AGGREGATION)))));
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return pattern;
        }

        @Override
        public Result apply(FilterNode node, Captures captures, Context context)
        {
            return pushFilter(node, captures.get(AGGREGATION), Optional.of(captures.get(PROJECT)), metadata, context);
        }
    }

    private static Rule.Result pushFilter(FilterNode filterNode, AggregationNode aggregationNode, Optional<ProjectNode> projectNode, Metadata metadata, Rule.Context context)
    {
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        VariableReferenceExpression countVariable = getOnlyElement(aggregationNode.getAggregations().keySet());
        Aggregation aggregation = getOnlyElement(aggregationNode.getAggregations().values());

        DomainTranslator domainTranslator = new RowExpressionDomainTranslator(metadata);
        DomainTranslator.ExtractionResult<VariableReferenceExpression> extractionResult =
                domainTranslator.fromPredicate(context.getSession().toConnectorSession(), filterNode.getPredicate(), BASIC_COLUMN_EXTRACTOR);

        TupleDomain<VariableReferenceExpression> tupleDomain = extractionResult.getTupleDomain();

        if (tupleDomain.isNone()) {
            // Filter predicate is never satisfied. Replace filter with empty values.
            return Rule.Result.ofPlanNode(new ValuesNode(
                    filterNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    filterNode.getOutputVariables(),
                    ImmutableList.of(),
                    Optional.empty()));
        }

        Domain countDomain = tupleDomain.getDomains().get().get(countVariable);
        if (countDomain == null) {
            // Filter predicate domain contains all countVariable values. Cannot filter out `0`.
            return Rule.Result.empty();
        }
        if (countDomain.contains(Domain.singleValue(countDomain.getType(), 0L))) {
            // Filter predicate domain contains `0`. Cannot filter out `0`.
            return Rule.Result.empty();
        }

        // Push down the aggregation's argument
        FilterNode source = new FilterNode(
                filterNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                aggregationNode.getSource(),
                aggregation.getArguments().get(0));

        // // Remove mask from the aggregation.
        // Aggregation newAggregation = new Aggregation(
        //         aggregation.getCall(),
        //         aggregation.getFilter(),
        //         aggregation.getOrderBy(),
        //         aggregation.isDistinct(),
        //         Optional.empty());

        AggregationNode newAggregationNode = new AggregationNode(
                aggregationNode.getSourceLocation(),
                aggregationNode.getId(),
                source,
                ImmutableMap.of(countVariable, aggregation),
                aggregationNode.getGroupingSets(),
                aggregationNode.getPreGroupedVariables(),
                aggregationNode.getStep(),
                aggregationNode.getHashVariable(),
                aggregationNode.getGroupIdVariable(),
                aggregationNode.getAggregationId());

        // Restore identity projection if it is present in the original plan.
        PlanNode filterSource = projectNode
                .map(project -> (PlanNode) project.replaceChildren(ImmutableList.of(newAggregationNode)))
                .orElse(newAggregationNode);

        // Try to simplify filter above the aggregation.
        if (countDomain.getValues().contains(ValueSet.ofRanges(Range.greaterThanOrEqual(countDomain.getType(), 1L)))) {
            // After filtering out `0` values, filter predicate's domain contains all remaining countVariable values.
            // Remove the countVariable domain.
            Map<VariableReferenceExpression, Domain> filteredDomains = tupleDomain.getDomains().get().entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(countVariable))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
            TupleDomain<VariableReferenceExpression> newTupleDomain = TupleDomain.withColumnDomains(filteredDomains);

            LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
                    new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                    new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()),
                    functionAndTypeManager);

            RowExpression newPredicate = logicalRowExpressions.combineConjuncts(
                    domainTranslator.toPredicate(newTupleDomain),
                    extractionResult.getRemainingExpression());

            if (newPredicate.equals(TRUE_CONSTANT)) {
                return Rule.Result.ofPlanNode(filterSource);
            }
            return Rule.Result.ofPlanNode(new FilterNode(
                    filterNode.getSourceLocation(),
                    filterNode.getId(),
                    filterSource,
                    newPredicate));
        }

        // Filter predicate cannot be simplified.
        return Rule.Result.ofPlanNode(filterNode.replaceChildren(ImmutableList.of(filterSource)));
    }

    private static boolean isGroupedCountWithMask(AggregationNode aggregationNode, FunctionResolution functionResolution)
    {
        if (!isGroupedAggregation(aggregationNode)) {
            return false;
        }
        if (aggregationNode.getAggregations().size() != 1) {
            return false;
        }
        Aggregation aggregation = getOnlyElement(aggregationNode.getAggregations().values());

        if (aggregation.getMask().isPresent() || aggregation.getFilter().isPresent()) {
            return false;
        }

        // Check it's count(*) — no arguments and is the count function
        return functionResolution.isCountFunction(aggregation.getFunctionHandle());
    }

    private static boolean isGroupedAggregation(AggregationNode aggregationNode)
    {
        return aggregationNode.hasNonEmptyGroupingSet() &&
                aggregationNode.getGroupingSetCount() == 1 &&
                aggregationNode.getStep() == AggregationNode.Step.SINGLE;
    }
}
