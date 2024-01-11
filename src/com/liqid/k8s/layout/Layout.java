/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceType;
import com.liqid.sdk.Machine;

import java.util.HashMap;
import java.util.Map;

public class Layout {

    private final Map<Integer, MachineLayout> _machineLayouts = new HashMap<>();
    private UnassignedLayout _unassignedLayout;

    public Layout() {
        _unassignedLayout = new UnassignedLayout();
    }

    public static Layout createFromInventory(
        final LiqidInventory inventory,
        final Integer groupId
    ) {
        var layout = new Layout();

        for (var machine : inventory._machinesById.values()) {
            var machLayout = new MachineLayout(machine);
            for (var ds : inventory._deviceStatusByMachineId.get(machine.getMachineId())) {
                if (ds.getDeviceType() != DeviceType.COMPUTE) {
                var di = inventory._deviceInfoByName.get(ds.getName());
                machLayout.getProfile().injectDevice(di);
                }
            }
            layout._machineLayouts.put(machine.getMachineId(), machLayout);
        }

        layout._unassignedLayout = new UnassignedLayout();
        for (var ds : inventory._deviceStatusByGroupId.get(groupId)) {
            if (ds.getDeviceType() != DeviceType.COMPUTE) {
                var rel = inventory._deviceRelationsByDeviceId.get(ds.getDeviceId());
                if ((rel != null) && (rel._machineId == null)) {
                    var di = inventory._deviceInfoByName.get(ds.getName());
                    layout._unassignedLayout.getProfile().injectDevice(di);
                }
            }
        }

        return layout;
    }

    public void addMachineLayout(final MachineLayout ml) { _machineLayouts.put(ml.getMachine().getMachineId(), ml); }
    public MachineLayout getMachineLayout(final Integer machineId) { return _machineLayouts.get(machineId); }
    public UnassignedLayout getUnassignedLayout() { return _unassignedLayout; }

    public void show(
        final String indent
    ) {
        _unassignedLayout.show(indent);
        for (var ml : _machineLayouts.values()) {
            ml.show(indent);
        }
    }
}
