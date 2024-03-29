/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.actions.Action;
import com.liqid.k8s.plan.actions.NoOperationAction;
import com.liqid.sdk.Machine;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Does things that involve a set of variances.
 * This represents a complete picture of devices which are not where we want them to be,
 * providing a way to generate a set of actions which will change the configuration so that
 * the devices end up where they are wanted.
 */
public class VarianceSet {

    private final HashSet<Variance> _content = new HashSet<>();

    public void addVariance(final Variance value) { _content.add(value); }
    public Set<Variance> getVariances() { return _content; }

    /**
     * Given a populated inventory and a map of desired assignments,
     * we create a VarianceSet representing the variances between the reality and what is desired.
     * @param inventory the state of the configuration as it currently exists
     * @param allocations a map describing, per machine, where we want devices to be
     * @return a VarianceSet which describes how to get from the inventory to the assignments
     */
    public static VarianceSet createVarianceSet(
        final LiqidInventory inventory,
        final Collection<Allocation> allocations
    ) {
        var vs = new VarianceSet();

        var allocMap = new HashMap<Machine, Allocation>();
        for (var alloc : allocations) {
            var machine = inventory.getMachine(alloc.getMachineName());
            allocMap.put(machine, alloc);
        }

        for (var mach : inventory.getMachines()) {
            if (allocMap.containsKey(mach)) {
                var devItems = inventory.getDeviceItemsForMachine(mach.getMachineId());
                LiqidInventory.removeDeviceItemsOfType(devItems, GeneralType.CPU);
                var machHasResourceIds = LiqidInventory.getDeviceIdsFromItems(devItems);
                var machWantsResourceIds = allocMap.get(mach).getDeviceIdentifiers();

                var gainingIds = machWantsResourceIds.stream()
                                                      .filter(devId -> !machHasResourceIds.contains(devId))
                                                      .collect(Collectors.toCollection(LinkedList::new));

                var losingIds = machHasResourceIds.stream()
                                                  .filter(devId -> !machWantsResourceIds.contains(devId))
                                                  .collect(Collectors.toCollection(LinkedList::new));

                if (!gainingIds.isEmpty() || !losingIds.isEmpty()) {
                    vs._content.add(new Variance(mach, gainingIds, losingIds));
                }
            } else {
                var devItems = inventory.getDeviceItemsForMachine(mach.getMachineId());
                LiqidInventory.removeDeviceItemsOfType(devItems, GeneralType.CPU);
                if (!devItems.isEmpty()) {
                    vs._content.add(new Variance(mach, Collections.emptyList(), LiqidInventory.getDeviceIdsFromItems(devItems)));
                }
            }
        }

        return vs;
    }

    /**
     * Given a list of variances, return an action created from the first variance in the list which
     * can create one (removing that variance from the list).
     * If no variance can create an action, we bifurcate one of our Variance objects and try again.
     * Bifurcation *should* work - if it does not, something is badly wrong.
     * @return null only if we have no Variances; otherwise, we return an Action and update ourselves accordingly
     * @throws InternalErrorException if we cannot create an Action (due to no variances being able to create one)
     * and bifurcation does not accomplish anything.
     */
    public Action getAction(
        final LiqidInventory inventory,
        final Set<Integer> unassignedResources
    ) throws InternalErrorException {
        var iter = _content.iterator();
        while (iter.hasNext()) {
            var variance = iter.next();
            var action = variance.createAction(inventory, unassignedResources);
            if (action instanceof NoOperationAction) {
                //  variance has nothing to add or remove - so we got a do-nothing.
                //  remove the variance and try again.
                iter.remove();
                continue;
            }

            if (action != null) {
                //  we have an action. remove the variance and return the action.
                iter.remove();
                return action;
            }

            //  If we get here, the variance could not generate an action.
            //  Continue until we find one which *can* generate an action.
        }

        //  If we get here, we cannot generate any action.
        //  If we don't actually have any actions left, just return null.
        if (_content.isEmpty()) {
            return null;
        }

        //  Find the the first variance which has both additions and removals and bifurcate it,
        //  replacing it in _content with its component parts.
        //  If we can find nothing to bifurcate, then we are in a right pickle.
        iter = _content.iterator();
        while (iter.hasNext()) {
            var variance = iter.next();
            if (variance.canBifurcate()) {
                _content.remove(variance);
                _content.addAll(variance.bifurcate());
                return getAction(inventory, unassignedResources);
            }
        }

        throw new InternalErrorException("Deadlock in variance set");
    }

    public boolean isEmpty() { return _content.isEmpty(); }

    @Override
    public String toString() {
        return _content.toString();
    }
}
