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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VarianceSetTest {

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
