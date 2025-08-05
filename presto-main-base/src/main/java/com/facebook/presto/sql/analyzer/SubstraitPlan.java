package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
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
import com.google.common.collect.ImmutableList;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        return null;
    }

    private class Visitor
            extends InternalPlanVisitor<Rel, Void>
    {
        Metadata metadata;
        Session session;

        @Override
        public Rel visitPlan(PlanNode node, Void context)
        {
            return null;
        }

        @Override
        public Rel visitProject(ProjectNode node, Void context)
        {
            Rel sourceRel = node.getSource().accept(this, context);
            List<FieldReference> sourceOutputs = getSourceOutputs(sourceRel);
            // FIX ME, I need to add non-identity projections
            // b.project(b->fieldReferences(sourceOutputs),)
            //         // Convert the projections to Substrait expressions
            //         node.getAssignments().getExpressions().stream()
            //                 .map(x -> b.toSubstraitExpression(x, sourceOutputs))
            //                 .collect(Collectors.toList()),
            //         sourceRel);
            // return super.visitProject(node, context);

            return null;
        }

        private List<FieldReference> getSourceOutputs(Rel sourceRel)
        {
            return IntStream.range(0, sourceRel.getRecordType().fields().size())
                    .mapToObj(i -> FieldReference.newInputRelReference(i, sourceRel))
                    .collect(Collectors.toList());
        }

        @Override
        public Rel visitFilter(FilterNode node, Void context)
        {
            Rel sourceRel = node.getSource().accept(this, context);
            // We need sourceRels output variables to build the predicate
            return b.filter(
                    // Convert the predicate to a Substrait expression
                    x -> toSubstraitExpression(node.getPredicate(), getSourceOutputs(sourceRel)),
                    sourceRel);
        }

        private Expression toSubstraitExpression(RowExpression rowExpression,
                List<FieldReference> fields)
        {
            return null;
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
                    return R.decimal(((DecimalType)type).getPrecision(), ((DecimalType)type).getScale());
                case StandardTypes.VARCHAR:
                    return R.STRING;
                default:
                    throw new UnsupportedOperationException("Need to add mapping for type: " + type.getTypeSignature());
            }
        }
    }
}
