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

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.partitioningBy;

/**
 * Replace filter predicate conjuncts with underlying project expressions.
 * <p>
 * Transforms:
 * <pre>{@code
 * - Filter (a AND b > c)
 *     - Project
 *       a <- d IS NULL
 *       b <- b
 *       c <- c
 *         - source (b, c, d)
 * }</pre>
 * into:
 * <pre>{@code
 * - Project
 *   a <- TRUE
 *   b <- b
 *   c <- c
 *     - Filter (d IS NULL AND b > c)
 *         - Project
 *           b <- b
 *           c <- c
 *           d <- d
 *         - source (b, c, d)
 * }</pre>
 * In the preceding example, filter predicate conjunct {@code a} is replaced with
 * project expression {@code d IS NULL}.
 * Additionally:
 * - an identity assignment {@code d <- d} is added to the underlying projection in order
 * to expose the symbol {@code d} used by the rewritten filter predicate,
 * - the inlined assignment {@code a <- d IS NULL} is removed from the underlying projection.
 * - another projection is added above the rewritten FilterNode, assigning
 * TRUE_CONSTANT to the replaced variable {@code a}. It is needed to restore the original
 * output of the FilterNode. If the variable {@code a} is not referenced in the upstream plan,
 * the projection should be subsequently removed by other rules.
 * <p>
 * Note: project expressions are inlined only in case when the resulting variables
 * are referenced exactly once in the filter predicate. Otherwise, the resulting
 * plan could be less efficient due to duplicated computation. Also, inlining the
 * expression multiple times is not correct in case of non-deterministic expressions.
 */
public class InlineProjectIntoFilter
        implements Rule<FilterNode>
{
    private static final Capture<ProjectNode> PROJECTION = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(project().capturedAs(PROJECTION)));

    private final LogicalRowExpressions logicalRowExpressions;

    public InlineProjectIntoFilter(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()),
                functionAndTypeManager);
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECTION);

        List<RowExpression> filterConjuncts = extractConjuncts(node.getPredicate());

        Map<Boolean, List<RowExpression>> conjuncts = filterConjuncts.stream()
                .collect(partitioningBy(VariableReferenceExpression.class::isInstance));
        List<RowExpression> simpleConjuncts = conjuncts.get(true);
        List<RowExpression> complexConjuncts = conjuncts.get(false);

        // Do not inline expression if the variable is used multiple times in simple conjuncts.
        Set<RowExpression> simpleUniqueConjuncts = simpleConjuncts.stream()
                .collect(Collectors.groupingBy(identity(), counting()))
                .entrySet().stream()
                .filter(entry -> entry.getValue() == 1)
                .map(Map.Entry::getKey)
                .collect(toImmutableSet());

        Set<RowExpression> complexConjunctVariables = VariablesExtractor.extractUnique(complexConjuncts).stream()
                .map(RowExpression.class::cast)
                .collect(toImmutableSet());

        // Do not inline expression if the variable is used in complex conjuncts.
        Set<RowExpression> simpleConjunctsToInline = Sets.difference(simpleUniqueConjuncts, complexConjunctVariables);

        if (simpleConjunctsToInline.isEmpty()) {
            return Result.empty();
        }

        ImmutableList.Builder<RowExpression> newConjuncts = ImmutableList.builder();
        Assignments.Builder newAssignments = Assignments.builder();
        Map<VariableReferenceExpression, RowExpression> postFilterAssignments = new HashMap<>();

        for (RowExpression conjunct : filterConjuncts) {
            if (simpleConjunctsToInline.contains(conjunct)) {
                VariableReferenceExpression variable = (VariableReferenceExpression) conjunct;
                RowExpression expression = projectNode.getAssignments().getMap().get(variable);
                if (expression == null || expression instanceof VariableReferenceExpression) {
                    // expression == null -> The variable is not produced by the underlying projection (i.e. it is a correlation variable).
                    // expression instanceof VariableReferenceExpression -> Do not inline trivial projections.
                    newConjuncts.add(conjunct);
                }
                else {
                    newConjuncts.add(expression);
                    newAssignments.putAll(identityAssignments(VariablesExtractor.extractUnique(expression)));
                    postFilterAssignments.put(variable, TRUE_CONSTANT);
                }
            }
            else {
                newConjuncts.add(conjunct);
            }
        }

        if (postFilterAssignments.isEmpty()) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> postFilterVariables = postFilterAssignments.keySet();
        // Remove inlined expressions from the underlying projection.
        newAssignments.putAll(projectNode.getAssignments().filter(variable -> !postFilterVariables.contains(variable)));

        Map<VariableReferenceExpression, RowExpression> outputAssignments = new HashMap<>();
        outputAssignments.putAll(identityAssignments(node.getOutputVariables()).getMap());
        // Restore inlined variables.
        outputAssignments.putAll(postFilterAssignments);

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                new FilterNode(
                        node.getSourceLocation(),
                        node.getId(),
                        new ProjectNode(
                                projectNode.getId(),
                                projectNode.getSource(),
                                newAssignments.build()),
                        logicalRowExpressions.combineConjuncts(newConjuncts.build())),
                Assignments.builder()
                        .putAll(outputAssignments)
                        .build()));
    }
}
