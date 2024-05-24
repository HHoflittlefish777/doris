// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.ExprFdItem;
import org.apache.doris.nereids.properties.FdFactory;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.FunctionalDependencies;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Union;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Logical Union.
 */
public class LogicalUnion extends LogicalSetOperation implements Union, OutputPrunable {

    // in doris, we use union node to present one row relation
    private final List<List<NamedExpression>> constantExprsList;
    // When there is an agg on the union and there is a filter on the agg,
    // it is necessary to keep the filter on the agg and push the filter down to each child of the union.
    private final boolean hasPushedFilter;

    public LogicalUnion(Qualifier qualifier, List<Plan> children) {
        super(PlanType.LOGICAL_UNION, qualifier, children);
        this.hasPushedFilter = false;
        this.constantExprsList = ImmutableList.of();
    }

    public LogicalUnion(Qualifier qualifier, List<List<NamedExpression>> constantExprsList, List<Plan> children) {
        super(PlanType.LOGICAL_UNION, qualifier, children);
        this.hasPushedFilter = false;
        this.constantExprsList = constantExprsList;
    }

    public LogicalUnion(Qualifier qualifier, List<NamedExpression> outputs, List<List<SlotReference>> childrenOutputs,
            List<List<NamedExpression>> constantExprsList, boolean hasPushedFilter, List<Plan> children) {
        super(PlanType.LOGICAL_UNION, qualifier, outputs, childrenOutputs, children);
        this.hasPushedFilter = hasPushedFilter;
        this.constantExprsList = Utils.fastToImmutableList(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    public LogicalUnion(Qualifier qualifier, List<NamedExpression> outputs, List<List<SlotReference>> childrenOutputs,
            List<List<NamedExpression>> constantExprsList, boolean hasPushedFilter,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Plan> children) {
        super(PlanType.LOGICAL_UNION, qualifier, outputs, childrenOutputs,
                groupExpression, logicalProperties, children);
        this.hasPushedFilter = hasPushedFilter;
        this.constantExprsList = Utils.fastToImmutableList(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    public boolean hasPushedFilter() {
        return hasPushedFilter;
    }

    public List<List<NamedExpression>> getConstantExprsList() {
        return constantExprsList;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return constantExprsList.stream().flatMap(List::stream).collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalUnion",
                "qualifier", qualifier,
                "outputs", outputs,
                "regularChildrenOutputs", regularChildrenOutputs,
                "constantExprsList", constantExprsList,
                "hasPushedFilter", hasPushedFilter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalUnion that = (LogicalUnion) o;
        return super.equals(that) && hasPushedFilter == that.hasPushedFilter
                && Objects.equals(constantExprsList, that.constantExprsList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), hasPushedFilter, constantExprsList);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalUnion(this, context);
    }

    @Override
    public LogicalUnion withChildren(List<Plan> children) {
        return new LogicalUnion(qualifier, outputs, regularChildrenOutputs,
                constantExprsList, hasPushedFilter, children);
    }

    @Override
    public LogicalSetOperation withChildrenAndTheirOutputs(List<Plan> children,
            List<List<SlotReference>> childrenOutputs) {
        Preconditions.checkArgument(children.size() == childrenOutputs.size(),
                "children size %s is not equals with children outputs size %s",
                children.size(), childrenOutputs.size());
        return new LogicalUnion(qualifier, outputs, childrenOutputs, constantExprsList, hasPushedFilter, children);
    }

    @Override
    public LogicalUnion withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList, hasPushedFilter,
                groupExpression, Optional.of(getLogicalProperties()), children);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalUnion(qualifier, outputs, regularChildrenOutputs, constantExprsList, hasPushedFilter,
                groupExpression, logicalProperties, children);
    }

    @Override
    public LogicalUnion withNewOutputs(List<NamedExpression> newOutputs) {
        return new LogicalUnion(qualifier, newOutputs, regularChildrenOutputs, constantExprsList,
                hasPushedFilter, Optional.empty(), Optional.empty(), children);
    }

    public LogicalUnion withNewOutputsAndConstExprsList(List<NamedExpression> newOutputs,
            List<List<NamedExpression>> constantExprsList) {
        return new LogicalUnion(qualifier, newOutputs, regularChildrenOutputs, constantExprsList,
                hasPushedFilter, Optional.empty(), Optional.empty(), children);
    }

    public LogicalUnion withChildrenAndConstExprsList(List<Plan> children,
            List<List<SlotReference>> childrenOutputs, List<List<NamedExpression>> constantExprsList) {
        return new LogicalUnion(qualifier, outputs, childrenOutputs, constantExprsList, hasPushedFilter, children);
    }

    public LogicalUnion withNewOutputsChildrenAndConstExprsList(List<NamedExpression> newOutputs, List<Plan> children,
                                                                List<List<SlotReference>> childrenOutputs,
                                                                List<List<NamedExpression>> constantExprsList) {
        return new LogicalUnion(qualifier, newOutputs, childrenOutputs, constantExprsList,
                hasPushedFilter, Optional.empty(), Optional.empty(), children);
    }

    public LogicalUnion withAllQualifier() {
        return new LogicalUnion(Qualifier.ALL, outputs, regularChildrenOutputs, constantExprsList, hasPushedFilter,
                Optional.empty(), Optional.empty(), children);
    }

    @Override
    public LogicalUnion pruneOutputs(List<NamedExpression> prunedOutputs) {
        return withNewOutputs(prunedOutputs);
    }

    @Override
    public void computeUnique(FunctionalDependencies.Builder fdBuilder) {
        if (qualifier == Qualifier.DISTINCT) {
            fdBuilder.addUniqueSlot(ImmutableSet.copyOf(getOutput()));
        }
    }

    @Override
    public void computeUniform(FunctionalDependencies.Builder fdBuilder) {
        // don't propagate uniform slots
    }

    private List<Set<Integer>> mapSlotToIndex(Plan plan, List<Set<Slot>> equalSlotsList) {
        Map<Slot, Integer> slotToIndex = new HashMap<>();
        for (int i = 0; i < plan.getOutput().size(); i++) {
            slotToIndex.put(plan.getOutput().get(i), i);
        }
        List<Set<Integer>> equalSlotIndicesList = new ArrayList<>();
        for (Set<Slot> equalSlots : equalSlotsList) {
            Set<Integer> equalSlotIndices = new HashSet<>();
            for (Slot slot : equalSlots) {
                if (slotToIndex.containsKey(slot)) {
                    equalSlotIndices.add(slotToIndex.get(slot));
                }
            }
            if (equalSlotIndices.size() > 1) {
                equalSlotIndicesList.add(equalSlotIndices);
            }
        }
        return equalSlotIndicesList;
    }

    @Override
    public void computeEqualSet(FunctionalDependencies.Builder fdBuilder) {
        List<Set<Integer>> unionEqualSlotIndicesList = new ArrayList<>();

        for (Plan child : children) {
            List<Set<Slot>> childEqualSlotsList =
                    child.getLogicalProperties().getFunctionalDependencies().calAllEqualSet();
            List<Set<Integer>> childEqualSlotsIndicesList = mapSlotToIndex(child, childEqualSlotsList);
            if (unionEqualSlotIndicesList.isEmpty()) {
                unionEqualSlotIndicesList = childEqualSlotsIndicesList;
            } else {
                // Only all child of union has the equal pair, we keep the equal pair.
                // It means we should calculate the intersection of all child
                for (Set<Integer> childEqualSlotIndices : childEqualSlotsIndicesList) {
                    for (Set<Integer> unionEqualSlotIndices : unionEqualSlotIndicesList) {
                        if (Collections.disjoint(childEqualSlotIndices, unionEqualSlotIndices)) {
                            unionEqualSlotIndices.retainAll(childEqualSlotIndices);
                        }
                    }
                }
            }

            List<Slot> ouputList = getOutput();
            for (Set<Integer> equalSlotIndices : unionEqualSlotIndicesList) {
                if (equalSlotIndices.size() <= 1) {
                    continue;
                }
                int first = equalSlotIndices.iterator().next();
                for (int idx : equalSlotIndices) {
                    fdBuilder.addEqualPair(ouputList.get(first), ouputList.get(idx));
                }
            }
        }
    }

    @Override
    public ImmutableSet<FdItem> computeFdItems() {
        Set<NamedExpression> output = ImmutableSet.copyOf(getOutput());
        ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();

        ImmutableSet<SlotReference> exprs = output.stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .collect(ImmutableSet.toImmutableSet());

        if (qualifier == Qualifier.DISTINCT) {
            ExprFdItem fdItem = FdFactory.INSTANCE.createExprFdItem(exprs, true, exprs);
            builder.add(fdItem);
        }

        return builder.build();
    }
}
