/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.DeviceType;
import com.liqid.sdk.Machine;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AllocatorTest {

    private static final String GROUP_NAME = "DeepThought";
    private static final Integer GROUP_ID = 42;

    private static final String ARTHUR = "ArthurDent";
    private static final String FORD = "FordPrefect";
    private static final String TRILLIAN = "Trillian";
    private static final String ZAPHOD = "ZaphodBeeblebrox";

    private void createDevices(
        final DeviceType deviceType,
        final String vendor,
        final String model,
        final String deviceNamePrefix,
        final int firstDeviceId,
        final int count,
        final LiqidInventory inventory
    ) {
        int devId = firstDeviceId;
        for (int dx = 0; dx < count; dx++) {
            var devName = String.format("%s%d", deviceNamePrefix, devId);

            var devStatus = new DeviceStatus();
            devStatus.setDeviceId(devId);
            devStatus.setDeviceType(deviceType);
            devStatus.setName(devName);
            devStatus.setIndex(devId);

            var devInfo = new DeviceInfo();
            devInfo.setDeviceInfoType(deviceType);
            devInfo.setName(devName);
            devInfo.setModel(model);
            devInfo.setVendor(vendor);
            devInfo.setIndex(devId);
            devInfo.setDeviceIdentifier(devId);

            inventory.notifyDeviceCreated(devStatus, devInfo);

            devId++;
        }
    }

    private void createMachine(
        final Integer machineId,
        final String machineName,
        final Integer groupId,
        final LiqidInventory inventory
    ) {
        var machine = new Machine();
        machine.setMachineId(machineId);
        machine.setMachineName(machineName);
        machine.setIndex(machineId);
        machine.setGroupId(groupId);
        inventory.notifyMachineCreated(machine);
    }

    @Test
    public void getOrderedIdentifiers_SimpleSpecific() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 3, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 3, inventory);
        createDevices(DeviceType.GPU, "Intel", "A770", "gpu", 300, 3, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);

        var resModelL40 = new SpecificResourceModel(GeneralType.GPU, "NVidia", "L40");

        var expected = new Integer[]{ 200, 201, 202 };
        var expList = Arrays.asList(expected);
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resModelL40, Collections.emptyList(), 1);
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleVendor() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 3, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 3, inventory);
        createDevices(DeviceType.GPU, "Intel", "A770", "gpu", 300, 3, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);

        var resVendorNVidia = new VendorResourceModel(GeneralType.GPU, "NVidia");

        var expected = new Integer[]{ 100, 101, 102, 200, 201, 202 };
        var expList = Arrays.asList(expected);
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resVendorNVidia, Collections.emptyList(), 1);
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleGeneric() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 3, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 3, inventory);
        createDevices(DeviceType.GPU, "Intel", "A770", "gpu", 300, 3, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);

        var resGPU = new GenericResourceModel(GeneralType.GPU);

        var expected = new Integer[]{ 100, 101, 102, 200, 201, 202, 300, 301, 302 };
        var expList = Arrays.asList(expected);
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), 1);
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleGenericWithVendorRestrictions() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 3, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 3, inventory);
        createDevices(DeviceType.GPU, "Intel", "A770", "gpu", 300, 3, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);

        var resNVidia = new VendorResourceModel(GeneralType.GPU, "NVidia");
        var resGPU = new GenericResourceModel(GeneralType.GPU);

        var expected = new Integer[]{ 300, 301, 302 };
        var expList = Arrays.asList(expected);
        var restrictions = Arrays.asList(new ResourceModel[]{ resNVidia });
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, restrictions, 1);
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleGenericWithMultipleRestrictions() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 3, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 3, inventory);
        createDevices(DeviceType.GPU, "Intel", "A770", "gpu", 300, 3, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);

        var resIntel = new VendorResourceModel(GeneralType.GPU, "Intel");
        var resModelA100 = new SpecificResourceModel(GeneralType.GPU, "NVidia", "A100");
        var resGPU = new GenericResourceModel(GeneralType.GPU);

        var expected = new Integer[]{ 200, 201, 202 };
        var expList = Arrays.asList(expected);
        var restrictions = Arrays.asList(new ResourceModel[]{ resIntel, resModelA100 });
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, restrictions, 1);
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleConflictingRestrictions() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 3, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 3, inventory);
        createDevices(DeviceType.GPU, "Intel", "A770", "gpu", 300, 3, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);

        var resNVidia = new VendorResourceModel(GeneralType.GPU, "NVidia");
        var resIntel = new VendorResourceModel(GeneralType.GPU, "Intel");
        var resGPU = new GenericResourceModel(GeneralType.GPU);

        var expList = new LinkedList<Integer>();
        var restrictions = Arrays.asList(new ResourceModel[]{ resNVidia, resIntel });
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, restrictions, 1);
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiers_Complicated() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 4, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 4, inventory);
        createDevices(DeviceType.GPU, "NVidia", "P4", "gpu", 300, 4, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);
        createMachine(2, FORD, GROUP_ID, inventory);
        createMachine(3, TRILLIAN, GROUP_ID, inventory);
        inventory.notifyDeviceAssignedToMachine(100, 1);
        inventory.notifyDeviceAssignedToMachine(200, 1);
        inventory.notifyDeviceAssignedToMachine(300, 1);
        inventory.notifyDeviceAssignedToMachine(101, 2);
        inventory.notifyDeviceAssignedToMachine(102, 2);
        inventory.notifyDeviceAssignedToMachine(301, 3);
        inventory.notifyDeviceAssignedToMachine(302, 3);
        inventory.notifyDeviceAssignedToMachine(303, 3);

        var resGPU = new GenericResourceModel(GeneralType.GPU);
        var expected = new Integer[]{ 100, 200, 300, 103, 201, 202, 203, 101, 102, 301, 302, 303 };
        var expList = Arrays.asList(expected);
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), 1);
        assertEquals(expList, list);

        expected = new Integer[]{ 101, 102, 103, 201, 202, 203, 100, 200, 300, 301, 302, 303 };
        expList = Arrays.asList(expected);
        list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), 2);
        assertEquals(expList, list);

        expected = new Integer[]{ 301, 302, 303, 103, 201, 202, 203, 100, 101, 102, 200, 300 };
        expList = Arrays.asList(expected);
        list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), 3);
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiersOfVendor_Complicated() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 4, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 4, inventory);
        createDevices(DeviceType.GPU, "Intel", "A770", "gpu", 300, 4, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);
        createMachine(2, FORD, GROUP_ID, inventory);
        createMachine(3, TRILLIAN, GROUP_ID, inventory);
        inventory.notifyDeviceAssignedToMachine(100, 1);
        inventory.notifyDeviceAssignedToMachine(200, 1);
        inventory.notifyDeviceAssignedToMachine(300, 1);
        inventory.notifyDeviceAssignedToMachine(101, 2);
        inventory.notifyDeviceAssignedToMachine(102, 2);
        inventory.notifyDeviceAssignedToMachine(301, 3);
        inventory.notifyDeviceAssignedToMachine(302, 3);
        inventory.notifyDeviceAssignedToMachine(303, 3);

        var resNVidia = new VendorResourceModel(GeneralType.GPU, "NVidia");
        var expected = new Integer[]{ 100, 200, 103, 201, 202, 203, 101, 102 };
        var expList = Arrays.asList(expected);
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resNVidia, Collections.emptyList(), 1);
        assertEquals(expList, list);

        expected = new Integer[]{ 101, 102, 103, 201, 202, 203, 100, 200 };
        expList = Arrays.asList(expected);
        list = Allocator.getOrderedDeviceIdentifiers(inventory, resNVidia, Collections.emptyList(), 2);
        assertEquals(expList, list);

        expected = new Integer[]{ 103, 201, 202, 203, 100, 101, 102, 200 };
        expList = Arrays.asList(expected);
        list = Allocator.getOrderedDeviceIdentifiers(inventory, resNVidia, Collections.emptyList(), 3);
        assertEquals(expList, list);
    }

    @Test
    public void simpleAllocations_AllUnowned() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        // none of the devices are assigned to machines
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 3, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 3, inventory);
        createDevices(DeviceType.GPU, "Intel", "A770", "gpu", 300, 3, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);
        createMachine(2, FORD, GROUP_ID, inventory);
        createMachine(3, TRILLIAN, GROUP_ID, inventory);

        // each machine wants 2 GPUs of any vendor and model.
        var layout = new ClusterLayout();
        var resModel = new GenericResourceModel(GeneralType.GPU);
        var machProf1 = new MachineProfile(inventory.getMachine(1));
        var machProf2 = new MachineProfile(inventory.getMachine(2));
        var machProf3 = new MachineProfile(inventory.getMachine(3));
        machProf1.injectCount(resModel, 2);
        machProf2.injectCount(resModel, 2);
        machProf3.injectCount(resModel, 2);
        layout.addMachineProfile(machProf1);
        layout.addMachineProfile(machProf2);
        layout.addMachineProfile(machProf3);

        var allocator = new Allocator();
        allocator.populateAllocations(inventory, layout);
        var allocs = allocator.getAllocations();

        var expArray = new Integer[]{ 100, 101, 102, 200, 201, 202, 300, 301, 302 };
        var expList = Arrays.asList(expArray);
        assertEquals(1, allocs.values().size());
        for (var entry : allocs.entrySet()) {
            var allocsForResModel = entry.getValue();
            assertEquals(3, allocsForResModel.size());
            for (var alloc : allocsForResModel) {
                assertEquals((Integer)2, alloc.getCount());
                assertEquals(expList, alloc.getDeviceIdentifiers());
            }
        }
    }

    @Test
    public void complicatedAllocations() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 5, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 5, inventory);
        createDevices(DeviceType.GPU, "Intel", "A770", "gpu", 300, 5, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);
        createMachine(2, FORD, GROUP_ID, inventory);
        createMachine(3, TRILLIAN, GROUP_ID, inventory);
        createMachine(4, ZAPHOD, GROUP_ID, inventory);

        // Put two A100's in machine 1, two L40's in machine 2, two L40's in machine 3, and three A770's in machine 4
        inventory.notifyDeviceAssignedToMachine(100, 1);
        inventory.notifyDeviceAssignedToMachine(101, 1);
        inventory.notifyDeviceAssignedToMachine(200, 2);
        inventory.notifyDeviceAssignedToMachine(201, 2);
        inventory.notifyDeviceAssignedToMachine(202, 3);
        inventory.notifyDeviceAssignedToMachine(203, 3);
        inventory.notifyDeviceAssignedToMachine(300, 4);
        inventory.notifyDeviceAssignedToMachine(301, 4);
        inventory.notifyDeviceAssignedToMachine(304, 4);

        // Machine 1 will want 1 A100 and 2 L40's,
        // Machine 2 will want 4 Intels
        // Machine 3 will want an A100.
        // Machine 4 will take 4 of anything except A100's
        var layout = new ClusterLayout();

        {
            var machProf1 = new MachineProfile(inventory.getMachine(1));
            var resModel1a = new SpecificResourceModel(GeneralType.GPU, "NVidia", "A100");
            var resModel1b = new SpecificResourceModel(GeneralType.GPU, "NVidia", "L40");
            machProf1.injectCount(resModel1a, 1);
            machProf1.injectCount(resModel1b, 2);

            var machProf2 = new MachineProfile(inventory.getMachine(2));
            var resModel2 = new VendorResourceModel(GeneralType.GPU, "Intel");
            machProf2.injectCount(resModel2, 4);

            var machProf3 = new MachineProfile(inventory.getMachine(3));
            var resModel3 = new SpecificResourceModel(GeneralType.GPU, "NVidia", "A100");
            machProf3.injectCount(resModel3, 1);

            var machProf4 = new MachineProfile(inventory.getMachine(4));
            var resModel4a = new GenericResourceModel(GeneralType.GPU);
            var resModel4b = new SpecificResourceModel(GeneralType.GPU, "NVidia", "A100");
            machProf4.injectCount(resModel4a, 4);
            machProf4.injectCount(resModel4b, 0);

            layout.addMachineProfile(machProf1);
            layout.addMachineProfile(machProf2);
            layout.addMachineProfile(machProf3);
            layout.addMachineProfile(machProf4);
        }

        var allocator = new Allocator();
        allocator.populateAllocations(inventory, layout);
        var allocs = allocator.getAllocations();

        {
            assertEquals(4, allocs.values().size());

            /* NVidia A100 - Arthur wants 1, Trillian wants 1 */
            var iter = allocs.entrySet().iterator();
            var entry = iter.next();
            var resModel = entry.getKey();
            var allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.GPU, resModel.getGeneralType());
            assertEquals("NVidia", resModel.getVendorName());
            assertEquals("A100", resModel.getModelName());
            assertTrue(resModel instanceof SpecificResourceModel);
            assertEquals(2, allocations.size());

            var subIter = allocations.iterator();
            var alloc1 = subIter.next();
            assertEquals(ARTHUR, alloc1.getMachine().getMachineName());
            assertEquals((Integer) 1, alloc1.getCount());
            var expected = Arrays.asList(new Integer[]{ 100, 101, 102, 103, 104 });
            assertEquals(expected, alloc1.getDeviceIdentifiers());

            var alloc2 = subIter.next();
            assertEquals(TRILLIAN, alloc2.getMachine().getMachineName());
            assertEquals((Integer) 1, alloc2.getCount());
            expected = Arrays.asList(new Integer[]{ 102, 103, 104, 100, 101 });
            assertEquals(expected, alloc2.getDeviceIdentifiers());

            /* NVidia L40 - Arthur wants 2 */
            entry = iter.next();
            resModel = entry.getKey();
            allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.GPU, resModel.getGeneralType());
            assertEquals("NVidia", resModel.getVendorName());
            assertEquals("L40", resModel.getModelName());
            assertTrue(resModel instanceof SpecificResourceModel);
            assertEquals(1, allocations.size());

            subIter = allocations.iterator();
            var alloc = subIter.next();
            assertEquals(ARTHUR, alloc.getMachine().getMachineName());
            assertEquals((Integer) 2, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 204, 200, 201, 202, 203 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            /* Intel - Ford wants 4 */
            entry = iter.next();
            resModel = entry.getKey();
            allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.GPU, resModel.getGeneralType());
            assertEquals("Intel", resModel.getVendorName());
            assertTrue(resModel instanceof VendorResourceModel);
            assertEquals(1, allocations.size());

            subIter = allocations.iterator();
            alloc = subIter.next();
            assertEquals(FORD, alloc.getMachine().getMachineName());
            assertEquals((Integer) 4, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 302, 303, 300, 301, 304 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            /* Any GPU except A100's, 4 of them for Zaphod */
            entry = iter.next();
            resModel = entry.getKey();
            allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);
            assertTrue(resModel instanceof GenericResourceModel);
            assertEquals(1, allocations.size());

            subIter = allocations.iterator();
            alloc = subIter.next();
            assertEquals(ZAPHOD, alloc.getMachine().getMachineName());
            assertEquals((Integer) 4, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 300, 301, 304, 204, 302, 303, 200, 201, 202, 203 });
            assertEquals(expected, alloc.getDeviceIdentifiers());
        }
    }

    @Test
    public void complicatedAllocations_MultipleTypes() {
        var inventory = SimpleLiqidInventory.createInventory(0, 0, 0, GROUP_ID, GROUP_NAME, false);
        createDevices(DeviceType.GPU, "NVidia", "A100", "gpu", 100, 5, inventory);
        createDevices(DeviceType.GPU, "NVidia", "L40", "gpu", 200, 5, inventory);
        createDevices(DeviceType.GPU, "Intel", "A770", "gpu", 300, 5, inventory);
        createDevices(DeviceType.FPGA, "Zytel", "Z5", "fpga", 1000, 10, inventory);
        createDevices(DeviceType.SSD, "Micron", "Bear", "ssd", 2000, 5, inventory);
        createDevices(DeviceType.SSD, "Micron", "Snake", "ssd", 2100, 5, inventory);
        createMachine(1, ARTHUR, GROUP_ID, inventory);
        createMachine(2, FORD, GROUP_ID, inventory);
        createMachine(3, TRILLIAN, GROUP_ID, inventory);
        createMachine(4, ZAPHOD, GROUP_ID, inventory);

        // Put two A100's in machine 1, two L40's in machine 2, two L40's in machine 3, and three A770's in machine 4
        // All the FPGAs go in machine 4
        // All the Bears in machine 2, the Snakes in machine 3
        inventory.notifyDeviceAssignedToMachine(100, 1);
        inventory.notifyDeviceAssignedToMachine(101, 1);
        inventory.notifyDeviceAssignedToMachine(200, 2);
        inventory.notifyDeviceAssignedToMachine(201, 2);
        inventory.notifyDeviceAssignedToMachine(202, 3);
        inventory.notifyDeviceAssignedToMachine(203, 3);
        inventory.notifyDeviceAssignedToMachine(300, 4);
        inventory.notifyDeviceAssignedToMachine(301, 4);
        inventory.notifyDeviceAssignedToMachine(304, 4);
        inventory.notifyDeviceAssignedToMachine(1000, 4);
        inventory.notifyDeviceAssignedToMachine(1001, 4);
        inventory.notifyDeviceAssignedToMachine(1002, 4);
        inventory.notifyDeviceAssignedToMachine(1003, 4);
        inventory.notifyDeviceAssignedToMachine(1004, 4);
        inventory.notifyDeviceAssignedToMachine(1005, 4);
        inventory.notifyDeviceAssignedToMachine(1006, 4);
        inventory.notifyDeviceAssignedToMachine(1007, 4);
        inventory.notifyDeviceAssignedToMachine(1008, 4);
        inventory.notifyDeviceAssignedToMachine(1009, 4);
        inventory.notifyDeviceAssignedToMachine(2000, 2);
        inventory.notifyDeviceAssignedToMachine(2001, 2);
        inventory.notifyDeviceAssignedToMachine(2002, 2);
        inventory.notifyDeviceAssignedToMachine(2003, 2);
        inventory.notifyDeviceAssignedToMachine(2004, 2);
        inventory.notifyDeviceAssignedToMachine(2100, 3);
        inventory.notifyDeviceAssignedToMachine(2101, 3);
        inventory.notifyDeviceAssignedToMachine(2102, 3);
        inventory.notifyDeviceAssignedToMachine(2103, 3);
        inventory.notifyDeviceAssignedToMachine(2104, 3);

        // Machine 1 will want 1 A100 and 2 L40's, and two SSDs of any type
        // Machine 2 will want 4 Intels, and 2 SSDs which are *not* bears
        // Machine 3 will want an A100, and a bear and a snake and 10 FPGAs.
        // Machine 4 will take 4 of any GPUs except A100's, and one SSD which is *not* a snake
        var layout = new ClusterLayout();

        {
            /* Arthur */
            var machProf1 = new MachineProfile(inventory.getMachine(1));
            var resModel1a = new SpecificResourceModel(GeneralType.GPU, "NVidia", "A100");
            var resModel1b = new SpecificResourceModel(GeneralType.GPU, "NVidia", "L40");
            var resModel1c = new GenericResourceModel(GeneralType.SSD);
            machProf1.injectCount(resModel1a, 1);
            machProf1.injectCount(resModel1b, 2);
            machProf1.injectCount(resModel1c, 2);

            /* Ford */
            var machProf2 = new MachineProfile(inventory.getMachine(2));
            var resModel2a = new VendorResourceModel(GeneralType.GPU, "Intel");
            var resModel2b = new GenericResourceModel(GeneralType.SSD);
            var resModel2c = new SpecificResourceModel(GeneralType.SSD, "Micron", "Bear");
            machProf2.injectCount(resModel2a, 4);
            machProf2.injectCount(resModel2b, 2);
            machProf2.injectCount(resModel2c, 0);

            /* Trillian */
            var machProf3 = new MachineProfile(inventory.getMachine(3));
            var resModel3a = new SpecificResourceModel(GeneralType.GPU, "NVidia", "A100");
            var resModel3b = new SpecificResourceModel(GeneralType.SSD, "Micron", "Bear");
            var resModel3c = new SpecificResourceModel(GeneralType.SSD, "Micron", "Snake");
            var resModel3d = new GenericResourceModel(GeneralType.FPGA);
            machProf3.injectCount(resModel3a, 1);
            machProf3.injectCount(resModel3b, 1);
            machProf3.injectCount(resModel3c, 1);
            machProf3.injectCount(resModel3d, 10);

            /* Zaphod */
            var machProf4 = new MachineProfile(inventory.getMachine(4));
            var resModel4a = new GenericResourceModel(GeneralType.GPU);
            var resModel4b = new SpecificResourceModel(GeneralType.GPU, "NVidia", "A100");
            var resModel4c = new GenericResourceModel(GeneralType.SSD);
            var resModel4d = new SpecificResourceModel(GeneralType.SSD, "Micron", "Snake");
            machProf4.injectCount(resModel4a, 4);
            machProf4.injectCount(resModel4b, 0);
            machProf4.injectCount(resModel4c, 1);
            machProf4.injectCount(resModel4d, 0);
            layout.addMachineProfile(machProf1);
            layout.addMachineProfile(machProf2);
            layout.addMachineProfile(machProf3);
            layout.addMachineProfile(machProf4);
        }

        var allocator = new Allocator();
        allocator.populateAllocations(inventory, layout);
        var allocs = allocator.getAllocations();

        {
            assertEquals(8, allocs.values().size());

            /* Any FPGA - Trillian wants 10 of them */
            var iter = allocs.entrySet().iterator();
            var entry = iter.next();
            var resModel = entry.getKey();
            var allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.FPGA, resModel.getGeneralType());
            assertTrue(resModel instanceof GenericResourceModel);
            assertEquals(1, allocations.size());

            var subIter = allocations.iterator();
            var alloc = subIter.next();
            assertEquals(TRILLIAN, alloc.getMachine().getMachineName());
            assertEquals((Integer) 10, alloc.getCount());
            var expected = Arrays.asList(new Integer[]{ 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            /* NVidia A100 - Arthur wants 1, Trillian wants 1 */
            entry = iter.next();
            resModel = entry.getKey();
            allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.GPU, resModel.getGeneralType());
            assertEquals("NVidia", resModel.getVendorName());
            assertEquals("A100", resModel.getModelName());
            assertTrue(resModel instanceof SpecificResourceModel);
            assertEquals(2, allocations.size());

            subIter = allocations.iterator();
            alloc = subIter.next();
            assertEquals(ARTHUR, alloc.getMachine().getMachineName());
            assertEquals((Integer) 1, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 100, 101, 102, 103, 104 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            alloc = subIter.next();
            assertEquals(TRILLIAN, alloc.getMachine().getMachineName());
            assertEquals((Integer) 1, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 102, 103, 104, 100, 101 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            /* NVidia L40 - Arthur wants 2 */
            entry = iter.next();
            resModel = entry.getKey();
            allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.GPU, resModel.getGeneralType());
            assertEquals("NVidia", resModel.getVendorName());
            assertEquals("L40", resModel.getModelName());
            assertTrue(resModel instanceof SpecificResourceModel);
            assertEquals(1, allocations.size());

            subIter = allocations.iterator();
            alloc = subIter.next();
            assertEquals(ARTHUR, alloc.getMachine().getMachineName());
            assertEquals((Integer) 2, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 204, 200, 201, 202, 203 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            /* Intel - Ford wants 4 */
            entry = iter.next();
            resModel = entry.getKey();
            allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.GPU, resModel.getGeneralType());
            assertEquals("Intel", resModel.getVendorName());
            assertTrue(resModel instanceof VendorResourceModel);
            assertEquals(1, allocations.size());

            subIter = allocations.iterator();
            alloc = subIter.next();
            assertEquals(FORD, alloc.getMachine().getMachineName());
            assertEquals((Integer) 4, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 302, 303, 300, 301, 304 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            /* Any GPU except A100's, 4 of them for Zaphod */
            entry = iter.next();
            resModel = entry.getKey();
            allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.GPU, resModel.getGeneralType());
            assertTrue(resModel instanceof GenericResourceModel);
            assertEquals(1, allocations.size());

            subIter = allocations.iterator();
            alloc = subIter.next();
            assertEquals(ZAPHOD, alloc.getMachine().getMachineName());
            assertEquals((Integer) 4, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 300, 301, 304, 204, 302, 303, 200, 201, 202, 203 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            /* Micron Bear SSD - Trillian wants just 1 */
            entry = iter.next();
            resModel = entry.getKey();
            allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.SSD, resModel.getGeneralType());
            assertEquals("Micron", resModel.getVendorName());
            assertEquals("Bear", resModel.getModelName());
            assertTrue(resModel instanceof SpecificResourceModel);
            assertEquals(1, allocations.size());

            subIter = allocations.iterator();
            alloc = subIter.next();
            assertEquals(TRILLIAN, alloc.getMachine().getMachineName());
            assertEquals((Integer) 1, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 2000, 2001, 2002, 2003, 2004 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            /* Micron Snake SSD - Trillian wants 1 of these also */
            entry = iter.next();
            resModel = entry.getKey();
            allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.SSD, resModel.getGeneralType());
            assertEquals("Micron", resModel.getVendorName());
            assertEquals("Snake", resModel.getModelName());
            assertTrue(resModel instanceof SpecificResourceModel);
            assertEquals(1, allocations.size());

            subIter = allocations.iterator();
            alloc = subIter.next();
            assertEquals(TRILLIAN, alloc.getMachine().getMachineName());
            assertEquals((Integer) 1, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 2100, 2101, 2102, 2103, 2104 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            /* Any SSD - Arthur wants 2, Ford wants 2 which are not bears, and Zaphod wants 4 but no snakes */
            entry = iter.next();
            resModel = entry.getKey();
            allocations = entry.getValue();
            System.out.printf("%s <=> %s\n", resModel, allocations);

            assertEquals(GeneralType.SSD, resModel.getGeneralType());
            assertTrue(resModel instanceof GenericResourceModel);
            assertEquals(3, allocations.size());

            subIter = allocations.iterator();
            alloc = subIter.next();
            assertEquals(ARTHUR, alloc.getMachine().getMachineName());
            assertEquals((Integer) 2, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 2000, 2001, 2002, 2003, 2004, 2100, 2101, 2102, 2103, 2104 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            alloc = subIter.next();
            assertEquals(FORD, alloc.getMachine().getMachineName());
            assertEquals((Integer) 2, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 2100, 2101, 2102, 2103, 2104 });
            assertEquals(expected, alloc.getDeviceIdentifiers());

            alloc = subIter.next();
            assertEquals(ZAPHOD, alloc.getMachine().getMachineName());
            assertEquals((Integer) 1, alloc.getCount());
            expected = Arrays.asList(new Integer[]{ 2000, 2001, 2002, 2003, 2004 });
            assertEquals(expected, alloc.getDeviceIdentifiers());
        }
    }
}
