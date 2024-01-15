/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.DeviceType;
import com.liqid.sdk.Group;
import com.liqid.sdk.Machine;
import org.junit.Test;

import static org.junit.Assert.*;

public class ClusterLayoutTest {

    private static LiqidInventory createSimpleInventory(
        final int groupCount,
        final int machineCount,
        final int mainGroupId
    ) {
        var inv = new LiqidInventory();

        for (var gid = 1; gid <= groupCount; gid++) {
            var group = new Group();
            group.setGroupId(gid);
            group.setGroupName(String.format("Group%d", gid));
            inv.notifyGroupCreated(group);
        }

        for (var mid = 1; mid <= machineCount; mid++) {
            var machine = new Machine();
            machine.setMachineId(mid);
            machine.setMachineName(String.format("Machine%d", mid));
            machine.setGroupId(mainGroupId);
            inv.notifyMachineCreated(machine);
        }

        return inv;
    }

    private static DeviceStatus createDevice(
        final LiqidInventory inventory,
        final DeviceType deviceType,
        final Integer deviceId,
        final String deviceName,
        final String vendorName,
        final String modelName
    ) {
        var ds = new DeviceStatus();
        ds.setDeviceType(deviceType);
        ds.setDeviceId(deviceId);
        ds.setName(deviceName);

        var di = new DeviceInfo();
        di.setDeviceInfoType(deviceType);
        di.setDeviceIdentifier(deviceId);
        di.setName(deviceName);
        di.setVendor(vendorName);
        di.setModel(modelName);

        inventory.notifyDeviceCreated(ds, di);

        return ds;
    }

    @Test
    public void createFromEmptyInventory() {
        var mainGroupId = 1;
        var inv = createSimpleInventory(3, 5, mainGroupId);
        var layout = ClusterLayout.createFromInventory(inv, mainGroupId);

        assertEquals(0, layout.getUnassignedProfile().getResourceModels().size());
        var machProfs = layout.getMachineProfiles();
        for (var machProf : machProfs) {
            assertEquals(0, machProf.getResourceModels().size());
        }
    }

    @Test
    public void createFromInventoryWithUnassignedDevices() {
        var mainGroupId = 1;
        var inv = createSimpleInventory(3, 5, mainGroupId);
        var gpu0 = createDevice(inv, DeviceType.GPU, 0x0B00, "gpu0", "Liqid", "GPU1000");
        var gpu1 = createDevice(inv, DeviceType.GPU, 0x0B01, "gpu1", "Liqid", "GPU1000");
        var gpu2 = createDevice(inv, DeviceType.GPU, 0x0B02, "gpu2", "Liqid", "GPU2000");
        var fpga0 = createDevice(inv, DeviceType.FPGA, 1, "fpga0", "MOS Technologies", "6502");
        var fpga1 = createDevice(inv, DeviceType.FPGA, 2, "fpga1", "MOS Technologies", "6502");
        var fpga2 = createDevice(inv, DeviceType.FPGA, 3, "fpga2", "MOS Technologies", "6502");
        inv.notifyDeviceAssignedToGroup(gpu0.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(gpu1.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(gpu2.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga0.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga1.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga2.getDeviceId(), mainGroupId);

        var layout = ClusterLayout.createFromInventory(inv, mainGroupId);
        layout.getUnassignedProfile().show("");

        assertEquals(3, layout.getUnassignedProfile().getResourceModels().size());
    }

    @Test
    public void createFromInventoryWithAssignedDevices() {
        var mainGroupId = 1;
        var inv = createSimpleInventory(3, 5, mainGroupId);
        var gpu0 = createDevice(inv, DeviceType.GPU, 0x0B00, "gpu0", "Liqid", "GPU1000");
        var gpu1 = createDevice(inv, DeviceType.GPU, 0x0B01, "gpu1", "Liqid", "GPU1000");
        var gpu2 = createDevice(inv, DeviceType.GPU, 0x0B02, "gpu2", "Liqid", "GPU2000");
        var fpga0 = createDevice(inv, DeviceType.FPGA, 1, "fpga0", "MOS Technologies", "6502");
        var fpga1 = createDevice(inv, DeviceType.FPGA, 2, "fpga1", "MOS Technologies", "6502");
        var fpga2 = createDevice(inv, DeviceType.FPGA, 3, "fpga2", "MOS Technologies", "6502");
        var fpga3 = createDevice(inv, DeviceType.FPGA, 4, "fpga3", "MOS Technologies", "6510");
        var fpga4 = createDevice(inv, DeviceType.FPGA, 5, "fpga4", "MOS Technologies", "6510");
        var fpga5 = createDevice(inv, DeviceType.FPGA, 6, "fpga5", "MOS Technologies", "6510");
        inv.notifyDeviceAssignedToGroup(gpu0.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(gpu1.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(gpu2.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga0.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga1.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga2.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga3.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga4.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga5.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToMachine(gpu0.getDeviceId(), 1);
        inv.notifyDeviceAssignedToMachine(gpu1.getDeviceId(), 2);
        inv.notifyDeviceAssignedToMachine(fpga0.getDeviceId(), 2);
        inv.notifyDeviceAssignedToMachine(fpga1.getDeviceId(), 2);
        inv.notifyDeviceAssignedToMachine(fpga2.getDeviceId(), 3);
        inv.notifyDeviceAssignedToMachine(fpga3.getDeviceId(), 3);
        inv.notifyDeviceAssignedToMachine(fpga4.getDeviceId(), 4);
        inv.notifyDeviceAssignedToMachine(fpga5.getDeviceId(), 4);

        var layout = ClusterLayout.createFromInventory(inv, mainGroupId);
        layout.show("");

        assertEquals(1, layout.getUnassignedProfile().getResourceModels().size());
        assertEquals(1, layout.getMachineProfile(1).getResourceModels().size());
        assertEquals(2, layout.getMachineProfile(2).getResourceModels().size());
        assertEquals(2, layout.getMachineProfile(3).getResourceModels().size());
        assertEquals(1, layout.getMachineProfile(4).getResourceModels().size());
        assertEquals(0, layout.getMachineProfile(5).getResourceModels().size());
    }

    @Test
    public void flatten_normal() {
        var mainGroupId = 1;
        var inv = createSimpleInventory(3, 5, mainGroupId);
        var gpu0 = createDevice(inv, DeviceType.GPU, 0x0B00, "gpu0", "Liqid", "GPU1000");
        var gpu1 = createDevice(inv, DeviceType.GPU, 0x0B01, "gpu1", "Liqid", "GPU1000");
        var gpu2 = createDevice(inv, DeviceType.GPU, 0x0B02, "gpu2", "Liqid", "GPU2000");
        var fpga0 = createDevice(inv, DeviceType.FPGA, 1, "fpga0", "MOS Technologies", "6502");
        var fpga1 = createDevice(inv, DeviceType.FPGA, 2, "fpga1", "MOS Technologies", "6502");
        var fpga2 = createDevice(inv, DeviceType.FPGA, 3, "fpga2", "MOS Technologies", "6502");
        var fpga3 = createDevice(inv, DeviceType.FPGA, 4, "fpga3", "MOS Technologies", "6510");
        var fpga4 = createDevice(inv, DeviceType.FPGA, 5, "fpga4", "MOS Technologies", "6510");
        var fpga5 = createDevice(inv, DeviceType.FPGA, 6, "fpga5", "MOS Technologies", "6510");
        inv.notifyDeviceAssignedToGroup(gpu0.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(gpu1.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(gpu2.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga0.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga1.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga2.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga3.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga4.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToGroup(fpga5.getDeviceId(), mainGroupId);
        inv.notifyDeviceAssignedToMachine(gpu0.getDeviceId(), 1);
        inv.notifyDeviceAssignedToMachine(gpu1.getDeviceId(), 2);
        inv.notifyDeviceAssignedToMachine(fpga0.getDeviceId(), 2);
        inv.notifyDeviceAssignedToMachine(fpga1.getDeviceId(), 2);
        inv.notifyDeviceAssignedToMachine(fpga2.getDeviceId(), 3);
        inv.notifyDeviceAssignedToMachine(fpga3.getDeviceId(), 3);
        inv.notifyDeviceAssignedToMachine(fpga4.getDeviceId(), 4);
        inv.notifyDeviceAssignedToMachine(fpga5.getDeviceId(), 4);

        var layout = ClusterLayout.createFromInventory(inv, mainGroupId).getFlattenedProfile();
        layout.show("");

        assertEquals(4, layout.getResourceModels().size());
    }

    @Test
    public void flatten_asRequested() {
        var mach1 = new Machine();
        mach1.setMachineName("Machine1");
        mach1.setMachineId(1);
        mach1.setGroupId(1);

        var mach2 = new Machine();
        mach2.setMachineName("Machine2");
        mach2.setMachineId(2);
        mach2.setGroupId(2);

        var mp1 = new MachineProfile(mach1);
        mp1.injectCount(new GenericResourceModel(GeneralType.GPU), 3);
        var mp2 = new MachineProfile(mach2);
        mp1.injectCount(new GenericResourceModel(GeneralType.GPU), 4);
        mp2.injectCount(new SpecificResourceModel(GeneralType.GPU, "Liqid", "Gen4"), 2);
        mp2.injectCount(new SpecificResourceModel(GeneralType.GPU, "Liqid", "Gen5"), 2);

        var layout = new ClusterLayout();
        layout.addMachineProfile(mp1);
        layout.addMachineProfile(mp2);

        var flat = layout.getFlattenedProfile();
        flat.show("");

        assertEquals(3, flat.getResourceModels().size());
    }
}
