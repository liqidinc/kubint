/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.Machine;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class contains logic to get us from the configuration we have, to the configuration we want.
 */
public class Allocator {

    // describes a particular allocation to be implemented
    public static class Allocation {

        // the machine to which this applied
        private final Machine _machine;

        // the number of devices to be allocated, from the given list
        private final Integer _count;

        // ordered list of device identifiers, from which we allocate.
        // we always allocate from the front of the list.
        private final List<Integer> _deviceIdentifiers = new LinkedList<>();

        public Allocation(
            final Machine machine,
            final Integer count,
            final List<Integer> deviceIdentifiers
        ) {
            _machine = machine;
            _count = count;
            _deviceIdentifiers.addAll(deviceIdentifiers);
        }

        public Machine getMachine() { return _machine; }
        public Integer getCount() { return _count; }
        public Collection<Integer> getDeviceIdentifiers() { return new LinkedList<>(_deviceIdentifiers); }

        @Override
        public String toString() {
            return String.format("%s:%d <- %s", _machine.getMachineName(), _count, _deviceIdentifiers);
        }
    }

    private final TreeMap<ResourceModel, LinkedList<Allocation>> _content = new TreeMap<>();

    public Map<ResourceModel, Collection<Allocation>> getAllocations() { return new TreeMap<>(_content); }

    /**
     * Creates a list of device identifiers from the inventory which are accepted by the given ResourceModel
     * object (i.e., match the general type, and the vendor (if relevant) and model (if relevant).
     * The ordering of the resulting list is important to the caller, and is as follows:
     *  Firstly, the devices owned by the given machine
     *  Secondly, the devices which are not owned by any machine
     *  Finally, the devices which are owned by other machines
     * Protected to facilitate unit tests.
     * @param inventory LiqidInventory from which we get the list of devices
     * @param resourceModel ResourceModel limiting the devices which we are allowed to consider
     * @param disallowedModels a possibly empty collection of ResourceModel objects which we are *not* allowed to
     *                         consider - the caller might want all GPUs (for example) *excepting* those from ACME,
     *                         or models T1 and T2 from SKY-NET.
     * @param machineId machine identifier of the machine to which this list applies
     * @return sorted list of device identifiers
     */
    protected static LinkedList<Integer> getOrderedDeviceIdentifiers(
        final LiqidInventory inventory,
        final ResourceModel resourceModel,
        final Collection<ResourceModel> disallowedModels,
        final Integer machineId
    ) {
        var thisMachineList = new LinkedList<Integer>();
        var otherMachineList = new LinkedList<Integer>();
        var freeList = new LinkedList<Integer>();

        for (var devItem : inventory.getDeviceItems()) {
            if (resourceModel.accepts(devItem.getDeviceInfo())) {
                var ignore = false;
                for (var rm : disallowedModels) {
                    if (rm.accepts(devItem.getDeviceInfo())) {
                        ignore = true;
                        break;
                    }
                }

                if (!ignore) {
                    var devId = devItem.getDeviceId();
                    if (devItem.isAssignedToMachine()) {
                        if (devItem.getMachineId().equals(machineId)) {
                            thisMachineList.add(devId);
                        } else {
                            otherMachineList.add(devId);
                        }
                    } else {
                        freeList.add(devId);
                    }
                }
            }
        }

        var result = new LinkedList<>(thisMachineList);
        result.addAll(freeList);
        result.addAll(otherMachineList);

        return result;
    }

    /**
     * Given an inventory of the current Liqid configuration and a desired layout, we populate our allocations list
     * with Allocation objects describing the potential devices for each ResourceModel entry indicated in the
     * desired layout.
     * This is auto-magically sorted such that the most specific resource models are first in line, followed
     * by the less-restrictive, and finishing with the least-restrictive. (Auto-magically, because our content
     * is sorted by ResourceModel, which has a compareTo() method implementing this sorting).
     * @param inventory LiqidInventory which sources the devices we consider
     * @param desiredLayout ClusterLayout which describes the layout wanted by the user
     */
    public void populateAllocations(
        final LiqidInventory inventory,
        final ClusterLayout desiredLayout
    ) {
        // iterate over the machine profiles in the desired layout.
        for (var machProf : desiredLayout.getMachineProfiles()) {
            var machineId = machProf.getMachine().getMachineId();
            var resModels = machProf.getResourceModels();

            // Find restrictive resource models (those with a value of zero)
            var restrictions = new HashSet<ResourceModel>();
            for (var rm : resModels) {
                var devCount = machProf.getCount(rm);
                if (devCount == 0) {
                    restrictions.add(rm);
                }
            }

            for (var rm : resModels) {
                var devCount = machProf.getCount(rm);
                if (devCount > 0) {
                    var devIds = getOrderedDeviceIdentifiers(inventory, rm, restrictions, machineId);
                    _content.computeIfAbsent(rm, k -> new LinkedList<>());
                    _content.get(rm).add(new Allocation(machProf.getMachine(), devCount, devIds));
                }
            }
        }
    }
}
