package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.TypeCreator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SubstraitPlan
{
    protected static final SimpleExtension.ExtensionCollection defaultExtensionCollection =
            SimpleExtension.loadDefaults();
    private static final PlanProtoConverter planToProto = new PlanProtoConverter();
    protected TypeCreator R = TypeCreator.REQUIRED;
    protected SubstraitBuilder b = new SubstraitBuilder(defaultExtensionCollection);
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
            return JsonFormat.printer().includingDefaultValueFields().print(protoPlan);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private static io.substrait.plan.Plan.Root buildRoot(Plan plan, Metadata metadata, Session session)
    {
        //TODO : Fix building the root
        return plan.getRoot().accept(new Visitor(metadata, session), null);
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
         * Get a map of the output variables of a PlanNode to the FieldReferences
         * There is an in-order 1:1 mapping between the output variables of a PlanNode and the sourceRel
         *
         * @param node
         * @return
         */
        private Map<VariableReferenceExpression, FieldReference> getNodeOutputToFieldRefMap(PlanNode node, Rel nodeAsRel)
        {
            ImmutableMap.Builder<VariableReferenceExpression, FieldReference> builder = ImmutableMap.builder();
            Preconditions.checkState(nodeAsRel.getRemap().isPresent(), "Remapping is not supported in Presto plans");
            for (int i = 0; i < node.getOutputVariables().size(); i++) {
                builder.put(node.getOutputVariables().get(i), b.fieldReference(nodeAsRel, i));
            }
            return builder.build();
        }

        @Override
        public Rel visitPlan(PlanNode node, Void context)
        {
            return null;
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
            Preconditions.checkState(sourceRel.getRemap().isPresent(), "Remapping is not supported in Presto plans");

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
            ConnectorTableHandle connectorHandle = tableHandle.getConnectorHandle();
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
            Set<String> columnNames = columnHandles.keySet();
            List<io.substrait.type.Type> columnTypes = columnHandles.values().stream()
                    .map(x -> toSubstraitType(metadata.getColumnMetadata(session, tableHandle, x).getType()))
                    .collect(ImmutableList.toImmutableList());

            // return b.namedScan()
            return b.namedScan(
                    // Ideally we would use a metadata method to get the table name from a TableHandle
                    // We need to add one to the SPI
                    Collections.singletonList(tableHandle.toString()),
                    columnNames,
                    columnTypes);
        }

        /**
         * Convert Presto type to Substrait type
         * See https://substrait.io/types/type_system/
         *
         * @param type
         * @return
         */
        private io.substrait.type.Type toSubstraitType(Type type)
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
    }
}
