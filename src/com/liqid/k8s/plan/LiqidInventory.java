/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan;

import com.bearsnake.klog.Logger;
import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidException;
import com.liqid.sdk.Machine;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Abstract class for all command handlers
 */
public class LiqidInventory {

    public static class DeviceRelation {
        public Integer _deviceId;
        public Integer _groupId;
        public Integer _machineId;

        public DeviceRelation(
            final Integer deviceId,
            final Integer groupId,
            final Integer machineId
        ) {
            _deviceId = deviceId;
            _groupId = (groupId != null) && (groupId.equals(0)) ? null : groupId;
            _machineId = (machineId != null) && (machineId.equals(0)) ? null : machineId;
        }
    }

    public final Map<Integer, DeviceInfo> _deviceInfoById = new HashMap<>();
    public final Map<String, DeviceInfo> _deviceInfoByName = new HashMap<>();
    public final Map<Integer, DeviceRelation> _deviceRelationsByDeviceId = new HashMap<>();
    public final Map<Integer, LinkedList<DeviceStatus>> _deviceStatusByGroupId = new HashMap<>();
    public final Map<Integer, DeviceStatus> _deviceStatusById = new HashMap<>();
    public final Map<Integer, LinkedList<DeviceStatus>> _deviceStatusByMachineId = new HashMap<>();
    public final Map<String, DeviceStatus> _deviceStatusByName = new HashMap<>();
    public final Map<Integer, Group> _groupsById = new HashMap<>();
    public final Map<String, Group> _groupsByName = new HashMap<>();
    public final Map<Integer, Machine> _machinesById = new HashMap<>();
    public final Map<String, Machine> _machinesByName = new HashMap<>();

    private LiqidInventory() {}

    /**
     * Loads the resource maps so we can do some auto-analysis
     * @throws LiqidException if we cannot communicate with the Liqid Cluster
     */
    public static LiqidInventory getLiqidInventory(
        final LiqidClient client,
        final Logger logger
    ) throws LiqidException {
        var fn = "getLiqidInventory";
        logger.trace("Entering %s", fn);

        var inv = new LiqidInventory();
        var devStats = client.getAllDevicesStatus();
        for (var ds : devStats) {
            inv._deviceStatusById.put(ds.getDeviceId(), ds);
            inv._deviceStatusByName.put(ds.getName(), ds);
            inv._deviceRelationsByDeviceId.put(ds.getDeviceId(), new DeviceRelation(ds.getDeviceId(), null, null));
        }

        var devInfos = new LinkedList<DeviceInfo>();
        devInfos.addAll(client.getComputeDeviceInfo());
        devInfos.addAll(client.getFPGADeviceInfo());
        devInfos.addAll(client.getGPUDeviceInfo());
        devInfos.addAll(client.getMemoryDeviceInfo());
        devInfos.addAll(client.getNetworkDeviceInfo());
        devInfos.addAll(client.getStorageDeviceInfo());

        for (var di : devInfos) {
            inv._deviceInfoById.put(di.getDeviceIdentifier(), di);
            inv._deviceInfoByName.put(di.getName(), di);
        }

        var machines = client.getMachines();
        for (Machine mach : machines) {
            inv._machinesById.put(mach.getMachineId(), mach);
            inv._machinesByName.put(mach.getMachineName(), mach);
            inv._deviceStatusByMachineId.put(mach.getMachineId(), new LinkedList<>());
        }

        var groups = client.getGroups();
        for (Group group : groups) {
            inv._groupsById.put(group.getGroupId(), group);
            inv._groupsByName.put(group.getGroupName(), group);
            inv._deviceStatusByGroupId.put(group.getGroupId(), new LinkedList<>());

            var preDevs = client.getDevices(null, group.getGroupId(), null);
            for (var preDev : preDevs) {
                var devStat = inv._deviceStatusByName.get(preDev.getDeviceName());
                inv._deviceStatusByGroupId.get(group.getGroupId()).add(devStat);
                var devRel = new DeviceRelation(devStat.getDeviceId(), preDev.getGroupId(), preDev.getMachineId());
                inv._deviceRelationsByDeviceId.put(devRel._deviceId, devRel);

                if (devRel._machineId != null) {
                    inv._deviceStatusByMachineId.get(devRel._machineId).add(devStat);
                }
            }
        }

        logger.trace("Exiting %s", fn);
        return inv;
    }
}
