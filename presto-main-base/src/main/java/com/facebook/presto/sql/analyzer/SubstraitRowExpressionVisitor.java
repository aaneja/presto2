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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.FunctionResolution;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.TypeCreator;

import java.util.Arrays;
import java.util.Map;

import static java.lang.String.format;

public class SubstraitRowExpressionVisitor
        implements RowExpressionVisitor<Expression, Map<VariableReferenceExpression, FieldReference>>
{
    protected static final SimpleExtension.ExtensionCollection defaultExtensionCollection =
            SimpleExtension.loadDefaults();
    static final TypeCreator R = TypeCreator.of(false);
    static final TypeCreator N = TypeCreator.of(true);
    private final FunctionAndTypeResolver functionAndTypeResolver;
    private final FunctionResolution functionResolution;
    protected SubstraitBuilder b = new SubstraitBuilder(defaultExtensionCollection);

    public SubstraitRowExpressionVisitor(FunctionAndTypeResolver functionAndTypeResolver)
    {
        this.functionAndTypeResolver = functionAndTypeResolver;
        functionResolution = new FunctionResolution(functionAndTypeResolver);
    }

    public static Expression.ScalarFunctionInvocation and(Expression... args)
    {
        // If any arg is nullable, the output of or is potentially nullable
        // For example: false or null = null
        boolean isOutputNullable = Arrays.stream(args).anyMatch(a -> a.getType().nullable());
        io.substrait.type.Type outputType = isOutputNullable ? N.BOOLEAN : R.BOOLEAN;
        return scalarFn(DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "and:bool", outputType, args);
    }

    public static Expression.ScalarFunctionInvocation scalarFn(
            String namespace, String key, io.substrait.type.Type outputType, FunctionArg... args)
    {
        SimpleExtension.ScalarFunctionVariant declaration =
                defaultExtensionCollection.getScalarFunction(SimpleExtension.FunctionAnchor.of(namespace, key));
        return Expression.ScalarFunctionInvocation.builder()
                .declaration(declaration)
                .outputType(outputType)
                .arguments(Arrays.stream(args).collect(java.util.stream.Collectors.toList()))
                .build();
    }

    @Override
    public Expression visitVariableReference(VariableReferenceExpression reference, Map<VariableReferenceExpression, FieldReference> context)
    {
        return context.get(reference);
    }

    @Override
    public Expression visitInputReference(InputReferenceExpression input, Map<VariableReferenceExpression, FieldReference> context)
    {
        throw new UnsupportedOperationException("Cant convert InputReferenceExpression to expression");
    }

    @Override
    public Expression visitCall(CallExpression call, Map<VariableReferenceExpression, FieldReference> context)
    {
        if (functionResolution.isEqualsFunction(call.getFunctionHandle())) {
            if (call.getArguments().size() != 2) {
                throw new IllegalArgumentException("Equals function must have exactly two arguments: " + call);
            }
            Expression left = call.getArguments().get(0).accept(this, context);
            Expression right = call.getArguments().get(1).accept(this, context);
            return b.equal(left, right);
        }
        else if (functionResolution.isNotFunction(call.getFunctionHandle())) {
            if (call.getArguments().size() != 1) {
                throw new IllegalArgumentException("Not function must have exactly one argument: " + call);
            }
            Expression arg = call.getArguments().get(0).accept(this, context);
            return scalarFn(DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "not:bool", R.BOOLEAN, arg);
        }

        QualifiedObjectName callName = functionAndTypeResolver.getFunctionMetadata(call.getFunctionHandle()).getName();
        if (callName.equals(OperatorType.ADD.getFunctionName())) {
            if (call.getArguments().size() != 2) {
                throw new IllegalArgumentException("Equals function must have exactly two arguments: " + call);
            }

            Expression left = call.getArguments().get(0).accept(this, context);
            Expression right = call.getArguments().get(1).accept(this, context);
            return b.add(left, right);
        }

        if (callName.equals(OperatorType.SUBTRACT.getFunctionName())) {
            if (call.getArguments().size() != 2) {
                throw new IllegalArgumentException("Equals function must have exactly two arguments: " + call);
            }

            Expression left = call.getArguments().get(0).accept(this, context);
            Expression right = call.getArguments().get(1).accept(this, context);
            return b.subtract(left, right);
        }

        throw new UnsupportedOperationException("Cant convert CallExpression to expression");
    }

    @Override
    public Expression visitConstant(ConstantExpression node, Map<VariableReferenceExpression, FieldReference> context)
    {
        Type type = node.getType();
        if (type instanceof BooleanType) {
            return b.bool((Boolean) node.getValue());
        }
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType
                || type instanceof IntegerType) {
            Number number = (Number) node.getValue();
            return Expression.I64Literal.builder().value(number.longValue()).build();
        }
        throw new UnsupportedOperationException("Unsupported constant type: " + type.getDisplayName() +
                " for constant: " + node.getValue() + " in expression: " + node);
    }

    @Override
    public Expression visitLambda(LambdaDefinitionExpression lambda, Map<VariableReferenceExpression, FieldReference> context)
    {
        throw new UnsupportedOperationException("Cant convert LambdaDefinitionExpression to expression");
    }

    @Override
    public Expression visitSpecialForm(SpecialFormExpression specialForm, Map<VariableReferenceExpression, FieldReference> context)
    {
        switch (specialForm.getForm()) {
            case AND:
                return and(specialForm.getArguments().stream().map(arg -> arg.accept(this, context)).toArray(Expression[]::new));
            case OR:
                return b.or(specialForm.getArguments().stream().map(arg -> arg.accept(this, context)).toArray(Expression[]::new));
            default:
                throw new UnsupportedOperationException(format("Unsupported special form: %s in expression: %s",
                        specialForm.getForm(), specialForm));
        }
    }
}
