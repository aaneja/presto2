package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Plan;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.PlanProtoConverter;

public class SubstraitPlan
{

    private static final PlanProtoConverter planToProto = new PlanProtoConverter();

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

    // private class Visitor
    //         extends InternalPlanVisitor<Void, Void>
    // {
    //
    // }
}
