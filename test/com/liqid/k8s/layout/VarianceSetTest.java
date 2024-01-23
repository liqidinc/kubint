/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.actions.Action;
import com.liqid.sdk.DeviceType;
import com.liqid.sdk.LiqidException;
import com.liqid.sdk.mock.MockLiqidClient;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VarianceSetTest {

    @Test
    public void createVarianceSet() throws LiqidException {
        var mock = new MockLiqidClient.Builder().build();
        var group = mock.createGroup("Group");
        var machine1 = mock.createMachine(group.getGroupId(), "Machine1");
        var machine2 = mock.createMachine(group.getGroupId(), "Machine2");
        var machine3 = mock.createMachine(group.getGroupId(), "Machine3");
        var machine4 = mock.createMachine(group.getGroupId(), "Machine4");

        var ids1 = mock.createDevices(DeviceType.GPU, (short)0x0010, (short)0x03, "NVidia", "A100", 4);
        var ids2 = mock.createDevices(DeviceType.GPU, (short)0x0010, (short)0x05, "NVidia", "L40", 4);
        var ids3 = mock.createDevices(DeviceType.GPU, (short)0x0015, (short)0x77, "Intel", "A770", 4);

        var allIds = new LinkedList<>(ids1);
        allIds.addAll(ids2);
        allIds.addAll(ids3);
        
        mock.groupPoolEdit(group.getGroupId());
        for (var id : allIds) {
            mock.addDeviceToGroup(id, group.getGroupId());
        }
        mock.groupPoolDone(group.getGroupId());

        mock.editFabric(machine1.getMachineId());
        mock.addDeviceToMachine(allIds.get(0), group.getGroupId(), machine1.getMachineId());
        mock.addDeviceToMachine(allIds.get(1), group.getGroupId(), machine1.getMachineId());
        mock.reprogramFabric(machine1.getMachineId());

        mock.editFabric(machine2.getMachineId());
        mock.addDeviceToMachine(allIds.get(4), group.getGroupId(), machine2.getMachineId());
        mock.addDeviceToMachine(allIds.get(5), group.getGroupId(), machine2.getMachineId());
        mock.reprogramFabric(machine2.getMachineId());

        mock.editFabric(machine3.getMachineId());
        mock.addDeviceToMachine(allIds.get(8), group.getGroupId(), machine3.getMachineId());
        mock.addDeviceToMachine(allIds.get(9), group.getGroupId(), machine3.getMachineId());
        mock.reprogramFabric(machine3.getMachineId());

        var inv = LiqidInventory.createLiqidInventory(mock);
        {
            var map = new HashMap<String, Collection<Integer>>();
            for (var m : inv.getMachines()) {
                var items = inv.getDeviceItemsForMachine(m.getMachineId());
                map.put(m.getMachineName(), LiqidInventory.getDeviceIdsFromItems(items));
            }
            System.out.println(map);
        }
        var alloc1 = new Allocation(machine1.getMachineName(),
                                    Arrays.asList(new Integer[]{ allIds.get(0), allIds.get(4), allIds.get(8) }));
        var alloc2 = new Allocation(machine2.getMachineName(),
                                    Arrays.asList(new Integer[]{ allIds.get(1), allIds.get(5), allIds.get(9) }));
        var alloc4 = new Allocation(machine4.getMachineName(),
                                    Arrays.asList(new Integer[]{ allIds.get(2), allIds.get(6), allIds.get(10) }));
        var allocations = Arrays.asList(new Allocation[]{ alloc1, alloc2, alloc4 });
        System.out.println(allocations);

        var vs = VarianceSet.createVarianceSet(inv, allocations);
        for (var v : vs.getVariances()) {
            if (v.getMachine().equals(machine1)) {
                var addExpected = Arrays.asList(new Integer[]{ allIds.get(4), allIds.get(8) });
                var removeExpected = Arrays.asList(new Integer[]{ allIds.get(1) });
                assertEquals(new TreeSet<>(addExpected), new TreeSet<>(v.getDeviceIdsToAdd()));
                assertEquals(new TreeSet<>(removeExpected), new TreeSet<>(v.getDeviceIdsToRemove()));
            } else if (v.getMachine().equals(machine2)) {
                var addExpected = Arrays.asList(new Integer[]{ allIds.get(1), allIds.get(9) });
                var removeExpected = Arrays.asList(new Integer[]{ allIds.get(4) });
                assertEquals(new TreeSet<>(addExpected), new TreeSet<>(v.getDeviceIdsToAdd()));
                assertEquals(new TreeSet<>(removeExpected), new TreeSet<>(v.getDeviceIdsToRemove()));
            } else if (v.getMachine().equals(machine3)) {
                var addExpected = Arrays.asList(new Integer[]{ });
                var removeExpected = Arrays.asList(new Integer[]{ allIds.get(8), allIds.get(9) });
                assertEquals(new TreeSet<>(addExpected), new TreeSet<>(v.getDeviceIdsToAdd()));
                assertEquals(new TreeSet<>(removeExpected), new TreeSet<>(v.getDeviceIdsToRemove()));
            } else if (v.getMachine().equals(machine4)) {
                var addExpected = Arrays.asList(new Integer[]{ allIds.get(2), allIds.get(6), allIds.get(10) });
                var removeExpected = Arrays.asList(new Integer[]{ });
                assertEquals(new TreeSet<>(addExpected), new TreeSet<>(v.getDeviceIdsToAdd()));
                assertEquals(new TreeSet<>(removeExpected), new TreeSet<>(v.getDeviceIdsToRemove()));
            }
        }
    }

    @Test
    public void emptyVarianceSet() throws InternalErrorException, LiqidException {
        var mock = new MockLiqidClient.Builder().build();
        mock.createDevices(DeviceType.COMPUTE, (short)0x0001, (short)6502, "MOS Technologies", "6502", 3);
        mock.createDevices(DeviceType.GPU, (short)0x0010, (short)0x03, "Vector Graphics", "VT-G", 3);
        var inv = LiqidInventory.createLiqidInventory(mock);

        var vs = new VarianceSet();
        assertTrue(vs.getVariances().isEmpty());
        assertNull(vs.getAction(inv, Collections.emptySet()));
    }

    @Test
    public void varianceSetWithEmptyVariances() throws InternalErrorException, LiqidException {
        //  This should cause bifurcation resulting in eventual exhaustion of all Variance objects,
        //  resulting finally in a null.
        var mock = new MockLiqidClient.Builder().build();
        mock.createDevices(DeviceType.COMPUTE, (short)0x0001, (short)6502, "MOS Technologies", "6502", 3);
        mock.createDevices(DeviceType.GPU, (short)0x0010, (short)0x03, "Vector Graphics", "VT-G", 3);
        var group = mock.createGroup("Kubernetes");
        mock.createMachine(group.getGroupId(), "DorkTower");
        var inv = LiqidInventory.createLiqidInventory(mock);

        var machine = inv.getMachines().iterator().next();
        var vs = new VarianceSet();
        for (var vc = 0; vc < 10; vc++) {
            vs.addVariance(new Variance(machine, Collections.emptyList(), Collections.emptyList()));
        }

        assertNull(vs.getAction(inv, Collections.emptySet()));
    }

    @Test
    public void varianceSetSwapDevices() throws InternalErrorException, LiqidException {
        //  Create the situation where two machines exist, and each machine wants the device
        //  which is assigned to the other.  We should end up with three actions - taking a device from one machine,
        //  then adding it to *and* removing the other device, to/from the other machine, and finally adding the other
        //  device back to the first machine.
        //  But we *start* with two variances, each with an add and a remove.
        var mock = new MockLiqidClient.Builder().build();
        var gpuIds = mock.createDevices(DeviceType.GPU, (short)0x0010, (short)0x03, "Vector Graphics", "VT-G", 3);
        var group = mock.createGroup("Kubernetes");
        var machine1 = mock.createMachine(group.getGroupId(), "DorkTower");
        var machine2 = mock.createMachine(group.getGroupId(), "PennyArcade");
        var inv = LiqidInventory.createLiqidInventory(mock);

        var iter = gpuIds.iterator();
        Integer devId1 = iter.next();
        Integer devId2 = iter.next();

        var adding1 = Collections.singleton(devId1);
        var adding2 = Collections.singleton(devId2);
        var removing1 = Collections.singleton(devId2);
        var removing2 = Collections.singleton(devId1);

        var var1 = new Variance(machine1, adding1, removing1);
        var var2 = new Variance(machine2, adding2, removing2);
        var vs = new VarianceSet();
        vs.addVariance(var1);
        vs.addVariance(var2);

        var actions = new LinkedList<Action>();
        var unassigned = new HashSet<Integer>();
        while (!vs.isEmpty()) {
            actions.add(vs.getAction(inv, unassigned));
        }

        for (var action : actions) {
            System.out.println(action);
        }
    }

    @Test
    public void varianceSetSwapDevices2() throws InternalErrorException, LiqidException {
        // A trickier version of the above
        var mock = new MockLiqidClient.Builder().build();
        var gpuIds = mock.createDevices(DeviceType.GPU, (short)0x0010, (short)0x03, "Vector Graphics", "VT-G", 5);
        var group = mock.createGroup("Peanuts");
        var machine1 = mock.createMachine(group.getGroupId(), "CharlieBrown");
        var machine2 = mock.createMachine(group.getGroupId(), "LinusVanPelt");
        var machine3 = mock.createMachine(group.getGroupId(), "LucyVanPelt");
        var machine4 = mock.createMachine(group.getGroupId(), "Snoopy");
        var inv = LiqidInventory.createLiqidInventory(mock);

        var iter = gpuIds.iterator();
        Integer devId1 = iter.next();
        Integer devId2 = iter.next();
        Integer devId3 = iter.next();
        Integer devId4 = iter.next();

        var adding1 = Collections.singleton(devId1);
        var adding2 = Collections.singleton(devId2);
        var adding3 = Collections.singleton(devId3);
        var adding4 = Collections.singleton(devId4);
        var removing1 = Collections.singleton(devId2);
        var removing2 = Collections.singleton(devId3);
        var removing3 = Collections.singleton(devId4);
        var removing4 = Collections.singleton(devId1);

        var var1 = new Variance(machine1, adding1, removing1);
        var var2 = new Variance(machine2, adding2, removing2);
        var var3 = new Variance(machine3, adding3, removing3);
        var var4 = new Variance(machine4, adding4, removing4);
        var vs = new VarianceSet();
        vs.addVariance(var1);
        vs.addVariance(var2);
        vs.addVariance(var3);
        vs.addVariance(var4);

        var actions = new LinkedList<Action>();
        var unassigned = new HashSet<Integer>();
        while (!vs.isEmpty()) {
            actions.add(vs.getAction(inv, unassigned));
        }


        for (var action : actions) {
            System.out.println(action);
        }
    }
}
