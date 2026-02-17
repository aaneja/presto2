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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.NULL_IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Simplify conditional expressions in filter predicate.
 * <p>
 * Replaces conditional expression with an expression evaluating to TRUE
 * if and only if the original expression evaluates to TRUE.
 * The rewritten expression might not be equivalent to the original
 * expression.
 */
public class SimplifyFilterPredicate
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FunctionAndTypeManager functionAndTypeManager;
    private final LogicalRowExpressions logicalRowExpressions;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public SimplifyFilterPredicate(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
        this.logicalRowExpressions = new LogicalRowExpressions(
                determinismEvaluator,
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
        List<RowExpression> conjuncts = extractConjuncts(node.getPredicate());
        ImmutableList.Builder<RowExpression> newConjuncts = ImmutableList.builder();

        boolean simplified = false;
        for (RowExpression conjunct : conjuncts) {
            Optional<RowExpression> simplifiedConjunct = trySimplifyConjunct(conjunct);

            if (simplifiedConjunct.isPresent()) {
                simplified = true;
                newConjuncts.add(simplifiedConjunct.get());
            }
            else {
                newConjuncts.add(conjunct);
            }
        }
        if (!simplified) {
            return Result.empty();
        }

        RowExpression predicate = logicalRowExpressions.combineConjuncts(newConjuncts.build());
        if (predicate instanceof ConstantExpression && ((ConstantExpression) predicate).isNull()) {
            predicate = FALSE_CONSTANT;
        }
        return Result.ofPlanNode(new FilterNode(
                node.getSourceLocation(),
                node.getId(),
                node.getSource(),
                predicate));
    }

    private Optional<RowExpression> trySimplifyConjunct(RowExpression conjunct)
    {
        if (!(conjunct instanceof SpecialFormExpression)) {
            return Optional.empty();
        }
        SpecialFormExpression specialForm = (SpecialFormExpression) conjunct;
        switch (specialForm.getForm()) {
            case NULL_IF:
                return simplifyNullIf(specialForm);
            case SWITCH:
                return simplifySwitch(specialForm);
            default:
                return Optional.empty();
        }
    }

    private Optional<RowExpression> simplifyNullIf(SpecialFormExpression nullIf)
    {
        // NULLIF(a, b) in a filter context is true iff a is true AND (b is false or null)
        RowExpression first = nullIf.getArguments().get(0);
        RowExpression second = nullIf.getArguments().get(1);
        return Optional.of(LogicalRowExpressions.and(first, isFalseOrNullPredicate(second)));
    }

    private Optional<RowExpression> simplifySwitch(SpecialFormExpression switchExpr)
    {
        List<RowExpression> arguments = switchExpr.getArguments();
        RowExpression operand = arguments.get(0);

        // Determine if this is a searched CASE (operand is TRUE constant) or simple CASE
        boolean isSearchedCase = operand.equals(TRUE_CONSTANT);

        if (isSearchedCase) {
            return simplifySearchedCase(switchExpr);
        }
        else {
            return simplifySimpleCase(switchExpr);
        }
    }

    private Optional<RowExpression> simplifySearchedCase(SpecialFormExpression switchExpr)
    {
        List<RowExpression> arguments = switchExpr.getArguments();
        // arguments: [TRUE, WHEN1, WHEN2, ..., defaultValue]
        List<SpecialFormExpression> whenClauses = new ArrayList<>();
        for (int i = 1; i < arguments.size() - 1; i++) {
            whenClauses.add((SpecialFormExpression) arguments.get(i));
        }
        RowExpression defaultValue = arguments.get(arguments.size() - 1);

        if (whenClauses.size() == 1) {
            // if-like expression
            RowExpression condition = whenClauses.get(0).getArguments().get(0);
            RowExpression trueValue = whenClauses.get(0).getArguments().get(1);
            return simplifyIfLike(condition, trueValue, defaultValue);
        }

        List<RowExpression> operands = whenClauses.stream()
                .map(w -> w.getArguments().get(0))
                .collect(toImmutableList());
        List<RowExpression> results = whenClauses.stream()
                .map(w -> w.getArguments().get(1))
                .collect(toImmutableList());
        long trueResultsCount = results.stream()
                .filter(result -> result.equals(TRUE_CONSTANT))
                .count();
        long notTrueResultsCount = results.stream()
                .filter(SimplifyFilterPredicate::isNotTrue)
                .count();

        // all results true
        if (trueResultsCount == results.size() && defaultValue.equals(TRUE_CONSTANT)) {
            return Optional.of(TRUE_CONSTANT);
        }
        // all results not true
        if (notTrueResultsCount == results.size() && isNotTrue(defaultValue)) {
            return Optional.of(FALSE_CONSTANT);
        }
        // one result true, and remaining results not true
        if (trueResultsCount == 1 && notTrueResultsCount == results.size() - 1 && isNotTrue(defaultValue)) {
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
            for (SpecialFormExpression whenClause : whenClauses) {
                RowExpression whenOperand = whenClause.getArguments().get(0);
                RowExpression result = whenClause.getArguments().get(1);
                if (isNotTrue(result)) {
                    builder.add(isFalseOrNullPredicate(whenOperand));
                }
                else {
                    builder.add(whenOperand);
                    return Optional.of(logicalRowExpressions.combineConjuncts(builder.build()));
                }
            }
        }
        // all results not true, and default true
        if (notTrueResultsCount == results.size() && defaultValue.equals(TRUE_CONSTANT)) {
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
            operands.forEach(op -> builder.add(isFalseOrNullPredicate(op)));
            return Optional.of(logicalRowExpressions.combineConjuncts(builder.build()));
        }
        // skip clauses with not true conditions
        List<RowExpression> newWhenClauses = new ArrayList<>();
        for (SpecialFormExpression whenClause : whenClauses) {
            RowExpression whenOperand = whenClause.getArguments().get(0);
            if (whenOperand.equals(TRUE_CONSTANT)) {
                RowExpression result = whenClause.getArguments().get(1);
                if (newWhenClauses.isEmpty()) {
                    return Optional.of(result);
                }
                return Optional.of(buildSearchedCase(newWhenClauses, result));
            }
            if (!isNotTrue(whenOperand)) {
                newWhenClauses.add(whenClause);
            }
        }
        if (newWhenClauses.isEmpty()) {
            return Optional.of(defaultValue);
        }
        if (newWhenClauses.size() < whenClauses.size()) {
            return Optional.of(buildSearchedCase(newWhenClauses, defaultValue));
        }
        return Optional.empty();
    }

    private Optional<RowExpression> simplifySimpleCase(SpecialFormExpression switchExpr)
    {
        List<RowExpression> arguments = switchExpr.getArguments();
        RowExpression operand = arguments.get(0);
        RowExpression defaultValue = arguments.get(arguments.size() - 1);

        // If operand is null constant, result is always the default
        if (operand instanceof ConstantExpression && ((ConstantExpression) operand).isNull()) {
            return Optional.of(defaultValue);
        }

        List<RowExpression> results = new ArrayList<>();
        for (int i = 1; i < arguments.size() - 1; i++) {
            SpecialFormExpression whenClause = (SpecialFormExpression) arguments.get(i);
            results.add(whenClause.getArguments().get(1));
        }

        if (results.stream().allMatch(result -> result.equals(TRUE_CONSTANT)) && defaultValue.equals(TRUE_CONSTANT)) {
            return Optional.of(TRUE_CONSTANT);
        }
        if (results.stream().allMatch(SimplifyFilterPredicate::isNotTrue) && isNotTrue(defaultValue)) {
            return Optional.of(FALSE_CONSTANT);
        }
        return Optional.empty();
    }

    private Optional<RowExpression> simplifyIfLike(RowExpression condition, RowExpression trueValue, RowExpression falseValue)
    {
        if (trueValue.equals(TRUE_CONSTANT) && isNotTrue(falseValue)) {
            return Optional.of(condition);
        }
        if (isNotTrue(trueValue) && falseValue.equals(TRUE_CONSTANT)) {
            return Optional.of(isFalseOrNullPredicate(condition));
        }
        if (falseValue.equals(trueValue) && determinismEvaluator.isDeterministic(trueValue)) {
            return Optional.of(trueValue);
        }
        if (isNotTrue(trueValue) && isNotTrue(falseValue)) {
            return Optional.of(FALSE_CONSTANT);
        }
        if (condition.equals(TRUE_CONSTANT)) {
            return Optional.of(trueValue);
        }
        if (isNotTrue(condition)) {
            return Optional.of(falseValue);
        }
        return Optional.empty();
    }

    private static boolean isNotTrue(RowExpression expression)
    {
        return expression.equals(FALSE_CONSTANT) ||
                (expression instanceof ConstantExpression && ((ConstantExpression) expression).isNull());
    }

    private RowExpression isFalseOrNullPredicate(RowExpression expression)
    {
        return LogicalRowExpressions.or(
                new SpecialFormExpression(IS_NULL, BOOLEAN, expression),
                Expressions.not(functionAndTypeManager, expression));
    }

    private static RowExpression buildSearchedCase(List<RowExpression> whenClauses, RowExpression defaultValue)
    {
        ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
        arguments.add(TRUE_CONSTANT);
        arguments.addAll(whenClauses);
        arguments.add(defaultValue);
        return new SpecialFormExpression(SWITCH, BOOLEAN, arguments.build());
    }
}
