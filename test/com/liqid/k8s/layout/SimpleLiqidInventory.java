package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.DeviceType;
import com.liqid.sdk.Group;
import com.liqid.sdk.Machine;

public class SimpleLiqidInventory {

    public static final Integer DEFAULT_CPU_COUNT = 3;
    public static final Integer DEFAULT_GPU_COUNT = 12;
    public static final Integer DEFAULT_NIC_COUNT = 12;
    public static final Integer DEFAULT_GROUP_ID = 1;
    public static final String DEFAULT_GROUP_NAME = "Kubernetes";

    public static LiqidInventory createDefaultInventory() {
        return createInventory(DEFAULT_CPU_COUNT,
                               DEFAULT_GPU_COUNT,
                               DEFAULT_NIC_COUNT,
                               DEFAULT_GROUP_ID,
                               DEFAULT_GROUP_NAME,
                               false);
    }

    public static LiqidInventory createInventory(
        final Integer cpuCount,
        final Integer gpuCount,
        final Integer nicCount,
        final Integer groupId,
        final String groupName,
        final boolean assignDevices
    ) {
        var i = new LiqidInventory();

        var group = new Group();
        group.setGroupId(groupId);
        group.setGroupName(groupName);
        i.notifyGroupCreated(group);

        var cpuDevStats = new DeviceStatus[cpuCount];
        for (int cx = 0; cx < cpuCount; cx++) {
            var devId = 0x10001 + cx;
            var devName = String.format("pcpu%d", cx);

            var cpuDevInfo = new DeviceInfo();
            cpuDevInfo.setDeviceIdentifier(devId);
            cpuDevInfo.setName(devName);
            cpuDevInfo.setDeviceInfoType(DeviceType.COMPUTE);
            cpuDevInfo.setVendor("MOS Technologies");
            cpuDevInfo.setModel("6502");

            var cpuDevStat = new DeviceStatus();
            cpuDevStat.setDeviceId(devId);
            cpuDevStat.setName(devName);
            cpuDevStat.setDeviceType(DeviceType.COMPUTE);
            cpuDevStat.setIndex(cx);

            cpuDevStats[cx] = cpuDevStat;
            i.notifyDeviceCreated(cpuDevStat, cpuDevInfo);
            i.notifyDeviceAssignedToGroup(devId, groupId);
        }

        var gpuDevStats = new DeviceStatus[gpuCount];
        for (int gx = 0; gx < gpuCount; gx++) {
            var devId = 0x0b00 + gx;
            var devName = String.format("gpu%d", gx);

            var gpuDevInfo = new DeviceInfo();
            gpuDevInfo.setDeviceIdentifier(devId);
            gpuDevInfo.setName(devName);
            gpuDevInfo.setDeviceInfoType(DeviceType.GPU);
            gpuDevInfo.setVendor("Vector Graphics");
            gpuDevInfo.setModel("P200");

            var gpuDevStat = new DeviceStatus();
            gpuDevStat.setDeviceId(devId);
            gpuDevStat.setName(devName);
            gpuDevStat.setDeviceType(DeviceType.GPU);
            gpuDevStat.setIndex(gx);

            gpuDevStats[gx] = gpuDevStat;
            i.notifyDeviceCreated(gpuDevStat, gpuDevInfo);
            i.notifyDeviceAssignedToGroup(devId, groupId);
        }

        var nicDevStats = new DeviceStatus[nicCount];
        for (int nx = 0; nx < nicCount; nx++) {
            var devId = 0xF0001 + nx;
            var devName = String.format("nic%d", nx);

            var nicDevInfo = new DeviceInfo();
            nicDevInfo.setDeviceIdentifier(devId);
            nicDevInfo.setName(devName);
            nicDevInfo.setDeviceInfoType(DeviceType.ETHERNET_LINK);
            nicDevInfo.setVendor("EquiLink Inc");
            nicDevInfo.setModel("EL1599a");

            var nicDevStat = new DeviceStatus();
            nicDevStat.setDeviceId(devId);
            nicDevStat.setName(devName);
            nicDevStat.setDeviceType(DeviceType.ETHERNET_LINK);
            nicDevStat.setIndex(nx);

            nicDevStats[nx] = nicDevStat;
            i.notifyDeviceCreated(nicDevStat, nicDevInfo);
            i.notifyDeviceAssignedToGroup(devId, groupId);
        }

        var machines = new Machine[cpuCount];
        for (int mx = 0; mx < cpuCount; mx++) {
            var machId = mx + 1;
            var machName = String.format("Machine%d", mx);

            var machine = new Machine();
            machine.setMachineId(machId);
            machine.setMachineName(machName);
            machine.setIndex(mx);
            machine.setGroupId(groupId);
            machine.setComputeName(cpuDevStats[mx].getName());
            machines[mx] = machine;

            i.notifyMachineCreated(machine);
            i.notifyDeviceAssignedToMachine(cpuDevStats[mx].getDeviceId(), machId);
        }

        if (assignDevices) {
            var gx = 0;
            var mx = 0;
            var nx = 0;
            while (gx < gpuCount || nx < nicCount) {
                if (mx == cpuCount) {
                    mx = 0;
                }
                var machId = machines[mx++].getMachineId();
                if (gx < gpuCount) {
                    i.notifyDeviceAssignedToMachine(gpuDevStats[gx++].getDeviceId(), machId);
                }
                if (nx < nicCount) {
                    i.notifyDeviceAssignedToMachine(nicDevStats[nx++].getDeviceId(), machId);
                }
            }
        }

        return i;
    }
}
