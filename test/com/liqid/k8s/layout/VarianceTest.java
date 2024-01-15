/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.k8s.plan.actions.AssignToMachine;
import com.liqid.k8s.plan.actions.NoOperation;
import com.liqid.k8s.plan.actions.ReconfigureMachine;
import com.liqid.k8s.plan.actions.RemoveFromMachine;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VarianceTest {

    private static Set<Integer> divideIds(
        final Collection<Integer> original
    ) {
        var result = new HashSet<Integer>();
        while (result.size() < original.size()) {
            var id = original.iterator().next();
            original.remove(id);
            result.add(id);
        }
        return result;
    }

    /**
     * Retrieves collection of device identifiers for the devices in the given inventory of a given type
     * @param inventory source of DeviceItem objects
     * @param deviceType general device type
     * @return collection of device identifiers
     */
    private static Collection<Integer> getDeviceIdsOfType(
        final LiqidInventory inventory,
        final GeneralType deviceType
    ) {
        var devItems = inventory.getDeviceItems();
        LiqidInventory.removeDeviceItemsNotOfType(devItems, deviceType);
        return devItems.stream()
                       .map(DeviceItem::getDeviceId)
                       .collect(Collectors.toCollection(HashSet::new));
    }

    @Test
    public void empty() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var v = new Variance(machine, Collections.emptyList(), Collections.emptyList());

        assertEquals(machine, v.getMachine());
        assertEquals(0, v.getDeviceIdsToAdd().size());
        assertEquals(0, v.getDeviceIdsToRemove().size());
        assertFalse(v.hasAdditions());
        assertFalse(v.hasRemovals());
        assertFalse(v.canBifurcate());
    }

    @Test
    public void addOnly() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var devIds = getDeviceIdsOfType(inv, GeneralType.GPU);
        var v = new Variance(machine, devIds, Collections.emptyList());

        assertEquals(machine, v.getMachine());
        assertEquals(devIds.size(), v.getDeviceIdsToAdd().size());
        assertEquals(0, v.getDeviceIdsToRemove().size());
        assertTrue(v.hasAdditions());
        assertFalse(v.hasRemovals());
        assertFalse(v.canBifurcate());
    }

    @Test
    public void removeOnly() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var devIds = getDeviceIdsOfType(inv, GeneralType.GPU);
        var v = new Variance(machine, Collections.emptyList(), devIds);

        assertEquals(machine, v.getMachine());
        assertEquals(0, v.getDeviceIdsToAdd().size());
        assertEquals(devIds.size(), v.getDeviceIdsToRemove().size());
        assertFalse(v.hasAdditions());
        assertTrue(v.hasRemovals());
        assertFalse(v.canBifurcate());
    }

    @Test
    public void normal() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var addIds = getDeviceIdsOfType(inv, GeneralType.GPU);
        var removeIds = divideIds(addIds);
        var v = new Variance(machine, addIds, removeIds);

        assertEquals(machine, v.getMachine());
        assertEquals(addIds.size(), v.getDeviceIdsToAdd().size());
        assertEquals(removeIds.size(), v.getDeviceIdsToRemove().size());
        assertTrue(v.hasAdditions());
        assertTrue(v.hasRemovals());
        assertTrue(v.canBifurcate());
    }

    @Test
    public void bifurcateEmptyVariance() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var v = new Variance(machine, Collections.emptyList(), Collections.emptyList());

        var variances = v.bifurcate();
        assertEquals(1, variances.size());
        assertEquals(variances.iterator().next(), v);
    }

    @Test
    public void bifurcateAddOnly() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var devIds = getDeviceIdsOfType(inv, GeneralType.GPU);
        var v = new Variance(machine, devIds, Collections.emptyList());

        var variances = v.bifurcate();
        assertEquals(1, variances.size());
        assertEquals(variances.iterator().next(), v);
    }

    @Test
    public void bifurcateRemoveOnly() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var devIds = getDeviceIdsOfType(inv, GeneralType.GPU);
        var v = new Variance(machine, Collections.emptyList(), devIds);

        var variances = v.bifurcate();
        assertEquals(1, variances.size());
        assertEquals(variances.iterator().next(), v);
    }

    @Test
    public void bifurcateNormal() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var addIds = getDeviceIdsOfType(inv, GeneralType.GPU);
        var removeIds = divideIds(addIds);
        var v = new Variance(machine, addIds, removeIds);

        var variances = v.bifurcate();
        assertEquals(2, variances.size());

        var iter = variances.iterator();
        var removeVar = iter.next();
        var addVar = iter.next();

        assertEquals(0, removeVar.getDeviceIdsToAdd().size());
        assertEquals(removeIds.size(), removeVar.getDeviceIdsToRemove().size());

        assertEquals(addIds.size(), addVar.getDeviceIdsToAdd().size());
        assertEquals(0, addVar.getDeviceIdsToRemove().size());
    }

    @Test
    public void createActionFail() {
        //  Test a Variance which can never be satisfied.
        //  Set up devices-to-be-added, but leave one of the IDs out of the unassigned container.
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var devIds = getDeviceIdsOfType(inv, GeneralType.GPU);
        var v = new Variance(machine, devIds, Collections.emptyList());

        //  set up unassigned devices to match ids-to-be-added, then remove one id
        var unassignedDevices = new HashSet<>(devIds);
        var iter = unassignedDevices.iterator();
        iter.next();
        iter.remove();

        // there should be no action
        var action = v.createAction(inv, unassignedDevices);
        assertNull(action);

        //  unassigned container should still be one less than the list of ids-to-be-added
        assertEquals(devIds.size() - 1, unassignedDevices.size());
    }

    @Test
    public void createActionAddOnly() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var devIds = getDeviceIdsOfType(inv, GeneralType.GPU);
        var v = new Variance(machine, devIds, Collections.emptyList());

        //  Put all the ids-to-be-added into the unassigned set
        HashSet<Integer> unassignedDevices = new HashSet<>(devIds);

        //  make sure the details of the action are accurate
        var action = v.createAction(inv, unassignedDevices);
        assertTrue(action instanceof AssignToMachine);
        assertEquals(machine.getMachineName(), ((AssignToMachine) action).getMachineName());
        assertEquals(inv.getDeviceNamesFromIds(devIds), ((AssignToMachine) action).getDeviceNames());

        //  make sure the device ids were removed from the unassigned set
        assertTrue(unassignedDevices.isEmpty());
    }

    @Test
    public void createActionRemoveOnly() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var devIds = getDeviceIdsOfType(inv, GeneralType.GPU);
        var v = new Variance(machine, Collections.emptyList(), devIds);

        //  Use an empty unassigned devices set
        HashSet<Integer> unassignedDevices = new HashSet<>();

        //  make sure the details of the action are accurate
        var action = v.createAction(inv, unassignedDevices);
        assertTrue(action instanceof RemoveFromMachine);
        assertEquals(machine.getMachineName(), ((RemoveFromMachine) action).getMachineName());
        assertEquals(inv.getDeviceNamesFromIds(devIds), ((RemoveFromMachine) action).getDeviceNames());

        //  make sure the unassigned set now matches the devices-to-be-removed
        assertEquals(devIds, unassignedDevices);
    }

    @Test
    public void createActionAddAndRemove() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var addIds = getDeviceIdsOfType(inv, GeneralType.GPU);
        var removeIds = divideIds(addIds);
        var v = new Variance(machine, addIds, removeIds);

        //  Put all the ids-to-be-added into the unassigned set
        HashSet<Integer> unassignedDevices = new HashSet<>(addIds);

        //  make sure the details of the action are accurate
        var action = v.createAction(inv, unassignedDevices);
        assertTrue(action instanceof ReconfigureMachine);
        assertEquals(machine.getMachineName(), ((ReconfigureMachine) action).getMachineName());
        assertEquals(inv.getDeviceNamesFromIds(addIds), ((ReconfigureMachine) action).getDeviceNamesToAdd());
        assertEquals(inv.getDeviceNamesFromIds(removeIds), ((ReconfigureMachine) action).getDeviceNamesToRemove());

        //  unassigned set should now match devices-to-be-removed
        assertEquals(removeIds, unassignedDevices);
    }

    @Test
    public void createActionEmpty() {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var v = new Variance(machine, Collections.emptyList(), Collections.emptyList());

        HashSet<Integer> unassignedDevices = new HashSet<>();
        var action = v.createAction(inv, unassignedDevices);

        assertTrue(action instanceof NoOperation);
    }
}
