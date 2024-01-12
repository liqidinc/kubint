/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceType;
import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ClusterLayout {

    private final Map<Integer, MachineProfile> _machineProfiles = new HashMap<>();
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

        for (var machine : inventory._machinesById.values()) {
            var machLayout = new MachineProfile(machine);
            for (var ds : inventory._deviceStatusByMachineId.get(machine.getMachineId())) {
                if (ds.getDeviceType() != DeviceType.COMPUTE) {
                    var di = inventory._deviceInfoByName.get(ds.getName());
                    machLayout.injectDevice(di);
                }
            }
            layout._machineProfiles.put(machine.getMachineId(), machLayout);
        }

        var devStatuses = groupId == null ? inventory._deviceStatusById.values()
                                          : inventory._deviceStatusByGroupId.get(groupId);
        for (var ds : devStatuses) {
            if (ds.getDeviceType() != DeviceType.COMPUTE) {
                var rel = inventory._deviceRelationsByDeviceId.get(ds.getDeviceId());
                if ((rel != null) && (rel._machineId == null)) {
                    var di = inventory._deviceInfoByName.get(ds.getName());
                    layout._unassignedProfile.injectDevice(di);
                }
            }
        }

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
        var inventory = LiqidInventory.getLiqidInventory(client);
        return createFromInventory(inventory, groupId);
    }

    /**
     * Adds a MachineProfile to this entity
     * @param machProfile profile to be added
     */
    public void addMachineProfile(
        final MachineProfile machProfile
    ) {
        _machineProfiles.put(machProfile.getMachine().getMachineId(), machProfile);
    }

    /**
     * Retrieves a particular MachineProfile given the machine id for the machine of interest
     */
    public MachineProfile getMachineProfile(
        final Integer machineId
    ) {
        return _machineProfiles.get(machineId);
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
        var prof = new Profile();
        _machineProfiles.values().forEach(prof::injectProfile);
        prof.injectProfile(_unassignedProfile);
        return prof;
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
        for (var machProf : _machineProfiles.values()) {
            System.out.println(indent + machProf.getMachine().getMachineName());
            machProf.show(indent + "  ");
        }
    }
}
