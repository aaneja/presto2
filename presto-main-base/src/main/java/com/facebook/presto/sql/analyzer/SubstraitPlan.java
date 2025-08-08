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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.TextFormat;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.relation.extensions.EmptyDetail;
import io.substrait.type.TypeCreator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SubstraitPlan
{
    private static final SimpleExtension.ExtensionCollection defaultExtensionCollection = SimpleExtension.loadDefaults();
    private static final PlanProtoConverter planToProto = new PlanProtoConverter();
    protected static TypeCreator R = TypeCreator.REQUIRED;
    protected static SubstraitBuilder b = new SubstraitBuilder(defaultExtensionCollection);
    protected ExtensionCollector functionCollector = new ExtensionCollector();
    protected RelProtoConverter relProtoConverter = new RelProtoConverter(functionCollector);
    protected ProtoRelConverter protoRelConverter =
            new ProtoRelConverter(functionCollector, defaultExtensionCollection);

    /**
     * Build a Subtrait PROTOJSON plan from a Presto plan
     *
     * @param plan
     * @param metadata
     * @param session
     * @return
     */
    public static String buildSubstraitPlan(Plan plan, Metadata metadata, Session session)
    {
        ImmutablePlan.Builder builder = io.substrait.plan.Plan.builder();
        builder.addRoots(buildRoot(plan, metadata, session));
        io.substrait.proto.PlanOrBuilder protoPlan = planToProto.toProto(builder.build());
        try {
            StringBuilder sb = new StringBuilder();
            TextFormat.printer().print(protoPlan, sb);
            return sb.toString();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static io.substrait.plan.Plan.Root buildRoot(Plan plan, Metadata metadata, Session session)
    {
        return io.substrait.plan.Plan.Root.builder()
                .input(plan.getRoot().accept(new Visitor(metadata, session), null))
                .build();
    }

    private static class Visitor
            extends InternalPlanVisitor<Rel, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final SubstraitRowExpressionVisitor substraitRowExpressionVisitor;

        public Visitor(Metadata metadata, Session session)
        {
            this.metadata = metadata;
            this.session = session;
            functionAndTypeManager = metadata.getFunctionAndTypeManager();
            substraitRowExpressionVisitor = new SubstraitRowExpressionVisitor(functionAndTypeManager.getFunctionAndTypeResolver());
        }

        /**
         * Convert Presto type to Substrait type
         * See https://substrait.io/types/type_system/
         *
         * @param type
         * @return
         */
        private static io.substrait.type.Type toSubstraitType(Type type)
        {
            switch (type.getTypeSignature().getBase()) {
                case StandardTypes.BIGINT:
                    return R.I64;
                case StandardTypes.INTEGER:
                    return R.I32;
                case StandardTypes.SMALLINT:
                    return R.I16;
                case StandardTypes.TINYINT:
                    return R.I8;
                case StandardTypes.BOOLEAN:
                    return R.BOOLEAN;
                case StandardTypes.DATE:
                    return R.DATE;
                case StandardTypes.DECIMAL:
                    return R.decimal(((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
                case StandardTypes.VARCHAR:
                    return R.STRING;
                default:
                    throw new UnsupportedOperationException("Need to add mapping for type: " + type.getTypeSignature());
            }
        }

        /**
         * Get a map of the output variables of a PlanNode to the FieldReferences
         * There is an in-order 1:1 mapping between the output variables of a PlanNode and the sourceRel
         *
         * @param node
         * @return
         */
        private Map<VariableReferenceExpression, FieldReference> getNodeOutputToFieldRefMap(PlanNode node, Rel nodeAsRel)
        {
            ImmutableMap.Builder<VariableReferenceExpression, FieldReference> builder = ImmutableMap.builder();
            Preconditions.checkState(!nodeAsRel.getRemap().isPresent(), "Remapping is not supported in Presto plans");
            for (int i = 0; i < node.getOutputVariables().size(); i++) {
                builder.put(node.getOutputVariables().get(i), b.fieldReference(nodeAsRel, i));
            }
            return builder.build();
        }

        @Override
        public Rel visitPlan(PlanNode node, Void context)
        {
            return ExtensionSingle.from(new EmptyDetail()
                                        {
                                            @Override
                                            public String toString()
                                            {
                                                return String.format("Unmapped plan node: %s", node.getClass().getSimpleName());
                                            }
                                        },
                    node.getSources().get(0).accept(this, context)).build();
        }

        /**
         * Project Node : I am not sure if I completely understood the semantics of remap in Substrait
         * Presto's Project nodes can also drop source outputs, so possibly we need to handle that
         * https://substrait.io/relations/basics/#emit-output-ordering
         *
         * @param node
         * @param context
         * @return
         */
        @Override
        public Rel visitProject(ProjectNode node, Void context)
        {
            Rel sourceRel = node.getSource().accept(this, context);
            Preconditions.checkState(!sourceRel.getRemap().isPresent(), "Remapping is not supported in Presto plans");

            ImmutableList.Builder<Expression> relAssignments = ImmutableList.builder();
            Map<VariableReferenceExpression, FieldReference> sourceVariableRefs = getNodeOutputToFieldRefMap(node.getSource(), sourceRel);
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
                VariableReferenceExpression outVarRef = entry.getKey();
                if (sourceVariableRefs.containsKey(outVarRef)) {
                    // Identity projection, just add the source field reference as-is
                    relAssignments.add(sourceVariableRefs.get(outVarRef));
                    continue;
                }
                // Convert the RowExpression to a Substrait expression and add it to the assignments
                relAssignments.add(toSubstraitExpression(entry.getValue(), sourceVariableRefs));
            }

            return b.project(
                    input -> relAssignments.build(),
                    b.remap(), // No remapping in Presto plans
                    sourceRel);
        }

        @Override
        public Rel visitFilter(FilterNode node, Void context)
        {
            Rel sourceRel = node.getSource().accept(this, context);
            return b.filter(
                    x -> toSubstraitExpression(node.getPredicate(), getNodeOutputToFieldRefMap(node.getSource(), sourceRel)),
                    sourceRel);
        }

        private Expression toSubstraitExpression(RowExpression rowExpression, Map<VariableReferenceExpression, FieldReference> variableScopeMap)
        {
            return rowExpression.accept(substraitRowExpressionVisitor, variableScopeMap);
        }

        @Override
        public Rel visitTableScan(TableScanNode node, Void context)
        {
            TableHandle tableHandle = node.getTable();
            Map<VariableReferenceExpression, ColumnHandle> assignments = node.getAssignments();
            Map<ColumnHandle, String> columnHandleToColumnNameMap = metadata.getColumnHandles(session, tableHandle).entrySet().stream()
                    .collect(ImmutableMap.toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));

            // Buid a in-order of outputvariables list of column names that we read from the TableScanNode
            List<String> readColumns = node.getOutputVariables().stream()
                    .map(v -> Objects.requireNonNull(columnHandleToColumnNameMap.get(assignments.get(v))))
                    .collect(ImmutableList.toImmutableList());

            // And their types
            List<io.substrait.type.Type> columnTypes = node.getOutputVariables().stream()
                    .map(x -> toSubstraitType(metadata.getColumnMetadata(session, tableHandle, assignments.get(x)).getType()))
                    .collect(ImmutableList.toImmutableList());

            return b.namedScan(
                    // Ideally we would use a metadata method to get the table name from a TableHandle
                    // We need to add one to the SPI
                    Collections.singletonList(tableHandle.toString()),
                    readColumns,
                    columnTypes);
        }
    }
}
