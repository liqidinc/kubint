/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceType;

import java.util.HashMap;
import java.util.Map;

public class ClusterLayout {

    private final Map<Integer, MachineProfile> _machineProfiles = new HashMap<>();
    private final Profile _unassignedProfile;

    public ClusterLayout() {
        _unassignedProfile = new Profile();
    }

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

        for (var ds : inventory._deviceStatusByGroupId.get(groupId)) {
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

    public void addMachineLayout(final MachineProfile ml) { _machineProfiles.put(ml.getMachine().getMachineId(), ml); }
    public MachineProfile getMachineProfile(final Integer machineId) { return _machineProfiles.get(machineId); }
    public Profile getUnassignedProfile() { return _unassignedProfile; }

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
