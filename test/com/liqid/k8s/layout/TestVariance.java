/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.k8s.plan.actions.AssignToMachine;
import com.liqid.k8s.plan.actions.NoOperation;
import com.liqid.k8s.plan.actions.ReconfigureMachine;
import com.liqid.k8s.plan.actions.RemoveFromMachine;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.Machine;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestVariance {

    private static final Integer MACHINE_ID = 90125;
    private static final String MACHINE_NAME = "Machine";
    private static final String NODE_NAME = "Node";
    
    private static final Machine MACHINE = new Machine();
    static {
        MACHINE.setMachineId(MACHINE_ID);
        MACHINE.setMachineName(MACHINE_NAME);
    }

    private static final Collection<Integer> IDS_TO_ADD = new LinkedList<>();
    static {
        IDS_TO_ADD.add(1);
        IDS_TO_ADD.add(2);
        IDS_TO_ADD.add(3);
    }

    private static final String[] DEVICE_NAMES_TO_ADD = new String[]{ "dev1", "dev2", "dev3" };

    private static final Collection<Integer> IDS_TO_REMOVE = new LinkedList<>();
    static {
        IDS_TO_REMOVE.add(5);
        IDS_TO_REMOVE.add(6);
        IDS_TO_REMOVE.add(7);
        IDS_TO_REMOVE.add(8);
    }

    private static final String[] DEVICE_NAMES_TO_REMOVE = new String[]{ "dev5", "dev6", "dev7", "dev8" };

    private static final LiqidInventory INVENTORY = new LiqidInventory();
    static {
        for (int did = 1; did <= 10; did++) {
            var ds = new DeviceStatus();
            ds.setDeviceId(did);
            ds.setName(String.format("dev%d", did));
            INVENTORY._deviceStatusById.put(did, ds);
        }
    }

    @Test
    public void empty() {
        var v = new Variance(MACHINE, NODE_NAME, Collections.emptyList(), Collections.emptyList());

        assertEquals(MACHINE, v.getMachine());
        assertEquals("Node", v.getNodeName());
        assertEquals(0, v.getDeviceIdsToAdd().size());
        assertEquals(0, v.getDeviceIdsToRemove().size());
    }

    @Test
    public void addOnly() {
        var v = new Variance(MACHINE, NODE_NAME, IDS_TO_ADD, Collections.emptyList());

        assertEquals(MACHINE, v.getMachine());
        assertEquals("Node", v.getNodeName());
        assertEquals(IDS_TO_ADD.size(), v.getDeviceIdsToAdd().size());
        assertEquals(0, v.getDeviceIdsToRemove().size());
    }

    @Test
    public void removeOnly() {
        var v = new Variance(MACHINE, NODE_NAME, Collections.emptyList(), IDS_TO_REMOVE);

        assertEquals(MACHINE, v.getMachine());
        assertEquals("Node", v.getNodeName());
        assertEquals(0, v.getDeviceIdsToAdd().size());
        assertEquals(IDS_TO_REMOVE.size(), v.getDeviceIdsToRemove().size());
    }

    @Test
    public void normal() {
        var v = new Variance(MACHINE, NODE_NAME, IDS_TO_ADD, IDS_TO_REMOVE);

        assertEquals(MACHINE, v.getMachine());
        assertEquals("Node", v.getNodeName());
        assertEquals(IDS_TO_ADD.size(), v.getDeviceIdsToAdd().size());
        assertEquals(IDS_TO_REMOVE.size(), v.getDeviceIdsToRemove().size());
    }

    @Test
    public void bifurcateEmpty() {
        var v = new Variance(MACHINE, NODE_NAME, Collections.emptyList(), Collections.emptyList());

        var variances = v.bifurcate();
        assertEquals(0, variances.size());
    }

    @Test
    public void bifurcateAddOnly() {
        var v = new Variance(MACHINE, NODE_NAME, IDS_TO_ADD, Collections.emptyList());

        var variances = new LinkedList<>(v.bifurcate());
        assertEquals(1, variances.size());

        var addVar = variances.get(0);
        assertEquals(IDS_TO_ADD.size(), addVar.getDeviceIdsToAdd().size());
        assertEquals(0, addVar.getDeviceIdsToRemove().size());
    }

    @Test
    public void bifurcateRemoveOnly() {
        var v = new Variance(MACHINE, NODE_NAME, Collections.emptyList(), IDS_TO_REMOVE);

        var variances = new LinkedList<>(v.bifurcate());
        assertEquals(1, variances.size());

        var removeVar = variances.get(0);
        assertEquals(0, removeVar.getDeviceIdsToAdd().size());
        assertEquals(IDS_TO_REMOVE.size(), removeVar.getDeviceIdsToRemove().size());
    }

    @Test
    public void bifurcateNormal() {
        var v = new Variance(MACHINE, NODE_NAME, IDS_TO_ADD, IDS_TO_REMOVE);

        var variances = new LinkedList<>(v.bifurcate());
        assertEquals(2, variances.size());

        var addVar = variances.get(1);
        assertEquals(IDS_TO_ADD.size(), addVar.getDeviceIdsToAdd().size());
        assertEquals(0, addVar.getDeviceIdsToRemove().size());

        var removeVar = variances.get(0);
        assertEquals(0, removeVar.getDeviceIdsToAdd().size());
        assertEquals(IDS_TO_REMOVE.size(), removeVar.getDeviceIdsToRemove().size());
    }

    @Test
    public void createActionAddOnly() {
        var v = new Variance(MACHINE, NODE_NAME, IDS_TO_ADD, Collections.emptyList());

        HashSet<Integer> unassignedDevices = new HashSet<>(IDS_TO_ADD);
        var action = v.createAction(INVENTORY, unassignedDevices);

        assertTrue(action instanceof AssignToMachine);
        assertEquals(MACHINE_NAME, ((AssignToMachine) action).getMachineName());
        assertEquals(NODE_NAME, ((AssignToMachine) action).getNodeName());
        assertArrayEquals(DEVICE_NAMES_TO_ADD, ((AssignToMachine) action).getDeviceNames().toArray(new String[0]));
        assertTrue(unassignedDevices.isEmpty());
    }

    @Test
    public void createActionFail() {
        var v = new Variance(MACHINE, NODE_NAME, IDS_TO_ADD, Collections.emptyList());

        HashSet<Integer> unassignedDevices = new HashSet<>(IDS_TO_ADD);
        unassignedDevices.remove(2);
        var action = v.createAction(INVENTORY, unassignedDevices);

        assertNull(action);
        assertEquals(2, unassignedDevices.size());
    }

    @Test
    public void createActionRemoveOnly() {
        var v = new Variance(MACHINE, NODE_NAME, Collections.emptyList(), IDS_TO_REMOVE);

        HashSet<Integer> unassignedDevices = new HashSet<>();
        var action = v.createAction(INVENTORY, unassignedDevices);

        assertTrue(action instanceof RemoveFromMachine);
        assertEquals(MACHINE_NAME, ((RemoveFromMachine) action).getMachineName());
        assertEquals(NODE_NAME, ((RemoveFromMachine) action).getNodeName());
        assertArrayEquals(DEVICE_NAMES_TO_REMOVE, ((RemoveFromMachine) action).getDeviceNames().toArray(new String[0]));
    }

    @Test
    public void createActionAddAndRemove() {
        var v = new Variance(MACHINE, NODE_NAME, IDS_TO_ADD, IDS_TO_REMOVE);

        HashSet<Integer> unassignedDevices = new HashSet<>(IDS_TO_ADD);
        var action = v.createAction(INVENTORY, unassignedDevices);

        assertTrue(action instanceof ReconfigureMachine);
        assertEquals(MACHINE_NAME, ((ReconfigureMachine) action).getMachineName());
        assertEquals(NODE_NAME, ((ReconfigureMachine) action).getNodeName());
        assertArrayEquals(DEVICE_NAMES_TO_REMOVE, ((ReconfigureMachine) action).getDeviceNamesToRemove().toArray(new String[0]));
        assertArrayEquals(DEVICE_NAMES_TO_ADD, ((ReconfigureMachine) action).getDeviceNamesToAdd().toArray(new String[0]));
    }

    @Test
    public void createActionEmpty() {
        var v = new Variance(MACHINE, NODE_NAME, Collections.emptyList(), Collections.emptyList());

        HashSet<Integer> unassignedDevices = new HashSet<>(IDS_TO_ADD);
        var action = v.createAction(INVENTORY, unassignedDevices);

        assertTrue(action instanceof NoOperation);
    }
}
