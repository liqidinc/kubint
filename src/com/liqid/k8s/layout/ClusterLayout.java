/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a pattern of resource models segregated by machines.
 * Unlike the LiqidInventory, the layout does not know about individual devices -
 * it tracks only types, vendors, models, and counts.
 */
public class ClusterLayout {

    // key for the following map is the machine name
    private final Map<String, MachineProfile> _machineProfiles = new HashMap<>();
    private final Profile _unassignedProfile = new Profile();

    /**
     * Constructs an object based on the content of the given populated LiqidInventory object.
     * @param inventory Inventory object
     * @param groupId group id of the particular group in which we are interested, or null for everything
     */
    public static ClusterLayout createFromInventory(
        final LiqidInventory inventory,
        final Integer groupId
    ) {
        var layout = new ClusterLayout();

        //  devices assigned to machines
        for (var machine : inventory.getMachines()) {
            var machProfile = new MachineProfile(machine.getMachineName());
            var machDevs = inventory.getDeviceItemsForMachine(machine.getMachineId());
            LiqidInventory.removeDeviceItemsOfType(machDevs, GeneralType.CPU);
            for (var devItem : inventory.getDeviceItemsForMachine(machine.getMachineId())) {
                machProfile.injectDevice(devItem);
            }
            layout._machineProfiles.put(machProfile.getMachineName(), machProfile);
        }

        //  non-compute devices not assigned to any machines
        var devItems = inventory.getDeviceItems();
        LiqidInventory.removeDeviceItemsOfType(devItems, GeneralType.CPU);
        if (groupId != null) {
            LiqidInventory.removeDeviceItemsNotInGroup(devItems, groupId);
        }
        LiqidInventory.removeDeviceItemsInAnyMachine(devItems);
        devItems.forEach(layout._unassignedProfile::injectDevice);

        return layout;
    }

    /**
     * A wrapper around createFromInventory() which populates a temporary LiqidInventory
     * and then uses that to build a ClusterLayout
     */
    public static ClusterLayout createFromInventory(
        final LiqidClient client,
        final Integer groupId
    ) throws LiqidException {
        var inventory = LiqidInventory.createLiqidInventory(client);
        return createFromInventory(inventory, groupId);
    }

    /**
     * Adds a MachineProfile to this entity
     * @param machProfile profile to be added
     */
    public void addMachineProfile(
        final MachineProfile machProfile
    ) {
        _machineProfiles.put(machProfile.getMachineName(), machProfile);
    }

    /**
     * Retrieves a particular MachineProfile given the machine name for the machine of interest
     */
    public MachineProfile getMachineProfile(
        final String machineName
    ) {
        return _machineProfiles.get(machineName);
    }

    /**
     * Retrieves the collection of MachineProfile objects
     */
    public Collection<MachineProfile> getMachineProfiles() {
        return _machineProfiles.values();
    }

    /**
     * Retrieves the unassigned MachineProfile - if none has been added, we'll return the empty one.
     */
    public Profile getUnassignedProfile() {
        return _unassignedProfile;
    }

    /**
     * Retrieves a new Profile object which contains aggregate resource-model separated counts
     * based on the individual counters in each of the machine profiles and the unassigned profile.
     */
    public Profile getFlattenedProfile() {
        var profile = new Profile();
        _machineProfiles.values().forEach(profile::injectProfile);
        profile.injectProfile(_unassignedProfile);
        return profile;
    }

    /**
     * Display content
     * @param indent assist with display formatting - this string is prepended to all output
     */
    public void show(
        final String indent
    ) {
        System.out.println(indent + "<unassigned>");
        _unassignedProfile.show(indent + "  ");
        for (var machineProfile : _machineProfiles.values()) {
            System.out.println(indent + machineProfile.getMachineName());
            machineProfile.show(indent + "  ");
        }
    }
}
