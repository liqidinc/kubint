/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.actions.Action;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VarianceSetTest {

    @Test
    public void emptyVarianceSet() throws InternalErrorException {
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var vs = new VarianceSet();
        assertTrue(vs.getVariances().isEmpty());
        assertNull(vs.getAction(inv, Collections.emptySet()));
    }

    @Test
    public void varianceSetWithEmptyVariances() throws InternalErrorException {
        //  This should cause bifurcation resulting in eventual exhaustion of all Variance objects,
        //  resulting finally in a null.
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machine = inv.getMachines().iterator().next();
        var vs = new VarianceSet();
        for (var vc = 0; vc < 10; vc++) {
            vs.addVariance(new Variance(machine, Collections.emptyList(), Collections.emptyList()));
        }

        assertNull(vs.getAction(inv, Collections.emptySet()));
    }

    @Test
    public void varianceSetSwapDevices() throws InternalErrorException {
        //  Create the situation where two machines exist, and each machine wants the device
        //  which is assigned to the other.  We should end up with three actions - taking a device from one machine,
        //  then adding it to *and* removing the other device, to/from the other machine, and finally adding the other
        //  device back to the first machine.
        //  But we *start* with two variances, each with an add and a remove.
        var inv = SimpleLiqidInventory.createDefaultInventory();
        var machIter = inv.getMachines().iterator();
        var machine1 = machIter.next();
        var machine2 = machIter.next();

        var devIter = inv.getDeviceItems().iterator();
        DeviceItem devItem1 = devIter.next();
        while (!devItem1.getGeneralType().equals(GeneralType.GPU)) {
            devItem1 = devIter.next();
        }
        DeviceItem devItem2 = devIter.next();
        while (!devItem2.getGeneralType().equals(GeneralType.GPU)) {
            devItem2 = devIter.next();
        }

        var adding1 = Collections.singleton(devItem1.getDeviceId());
        var adding2 = Collections.singleton(devItem2.getDeviceId());
        var removing1 = Collections.singleton(devItem2.getDeviceId());
        var removing2 = Collections.singleton(devItem1.getDeviceId());

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
    public void varianceSetSwapDevices2() throws InternalErrorException {
        // A trickier version of the above
        var inv = SimpleLiqidInventory.createInventory(4, 8, 0, 42, "Group", false);
        var machIter = inv.getMachines().iterator();
        var machine1 = machIter.next();
        var machine2 = machIter.next();
        var machine3 = machIter.next();
        var machine4 = machIter.next();

        var devIter = inv.getDeviceItems().iterator();
        DeviceItem devItem1 = devIter.next();
        while (!devItem1.getGeneralType().equals(GeneralType.GPU)) {
            devItem1 = devIter.next();
        }
        DeviceItem devItem2 = devIter.next();
        while (!devItem2.getGeneralType().equals(GeneralType.GPU)) {
            devItem2 = devIter.next();
        }
        DeviceItem devItem3 = devIter.next();
        while (!devItem3.getGeneralType().equals(GeneralType.GPU)) {
            devItem3 = devIter.next();
        }
        DeviceItem devItem4 = devIter.next();
        while (!devItem4.getGeneralType().equals(GeneralType.GPU)) {
            devItem4 = devIter.next();
        }

        var adding1 = Collections.singleton(devItem1.getDeviceId());
        var adding2 = Collections.singleton(devItem2.getDeviceId());
        var adding3 = Collections.singleton(devItem3.getDeviceId());
        var adding4 = Collections.singleton(devItem4.getDeviceId());
        var removing1 = Collections.singleton(devItem2.getDeviceId());
        var removing2 = Collections.singleton(devItem3.getDeviceId());
        var removing3 = Collections.singleton(devItem4.getDeviceId());
        var removing4 = Collections.singleton(devItem1.getDeviceId());

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
