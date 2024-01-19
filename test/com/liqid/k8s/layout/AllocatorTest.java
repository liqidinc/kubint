/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceType;
import com.liqid.sdk.LiqidException;
import com.liqid.sdk.mock.MockDevice;
import com.liqid.sdk.mock.MockGroup;
import com.liqid.sdk.mock.MockLiqidClient;
import com.liqid.sdk.mock.MockMachine;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AllocatorTest {

    private static final String GROUP_NAME = "DeepThought";

    private static final String ARTHUR = "ArthurDent";
    private static final String FORD = "FordPrefect";
    private static final String TRILLIAN = "Trillian";
    private static final String ZAPHOD = "ZaphodBeeblebrox";

    private static final String INTEL = "Intel Corporation";
    private static final short INTEL_ID = (short)0x8086;
    private static final String MICRON = "Micron, LTD";
    private static final short MICRON_ID = 0x3333;
    private static final String MOS = "MOS Technologies";
    private static final short MOS_ID = 0x0001;
    private static final String NVIDIA = "NVIDIA Corp.";
    private static final short NVIDIA_ID = 0x0955;
    private static final String ZYTEL = "Zytel, GMBH";
    private static final short ZYTEL_ID = 0x4444;

    private Collection<Integer> _z5Devs;
    private Collection<Integer> _a100Devs;
    private Collection<Integer> _l40Devs;
    private Collection<Integer> _a770Devs;
    private Collection<Integer> _cpuDevs;
    private Collection<Integer> _bearDevs;
    private Collection<Integer> _snakeDevs;

    private MockGroup _group;
    private MockMachine _arthurMachine;
    private MockMachine _fordMachine;
    private MockMachine _trillianMachine;
    private MockMachine _zaphodMachine;

    private MockLiqidClient createMock() throws LiqidException {
        var mock = new MockLiqidClient.Builder().build();
        _z5Devs = mock.createDevices(DeviceType.FPGA, ZYTEL_ID, (short)0x0042, ZYTEL, "Z5", 10);
        _a100Devs = mock.createDevices(DeviceType.GPU, NVIDIA_ID, (short)0x0100, NVIDIA, "A100", 5);
        _l40Devs = mock.createDevices(DeviceType.GPU, NVIDIA_ID, (short)0x0101, NVIDIA, "L40", 7);
        _a770Devs = mock.createDevices(DeviceType.GPU, INTEL_ID, (short)0x0102, INTEL, "A770", 10);
        _cpuDevs = mock.createDevices(DeviceType.COMPUTE, MOS_ID, (short)0x6502, MOS, "6502", 5);
        _bearDevs = mock.createDevices(DeviceType.SSD, MICRON_ID, (short)0x0001, MICRON, "Bear", 5);
        _snakeDevs = mock.createDevices(DeviceType.SSD, MICRON_ID, (short)0x0003, MICRON, "Snake", 5);

        _group = (MockGroup) mock.createGroup(GROUP_NAME);
        _arthurMachine = (MockMachine) mock.createMachine(_group.getGroupId(), ARTHUR);
        _fordMachine = (MockMachine) mock.createMachine(_group.getGroupId(), FORD);
        _trillianMachine = (MockMachine) mock.createMachine(_group.getGroupId(), TRILLIAN);
        _zaphodMachine = (MockMachine) mock.createMachine(_group.getGroupId(), ZAPHOD);
        return mock;
    }

    private static void filterDevicesForModel(
        final Collection<MockDevice> devices,
        final String vendorName,
        final String modelName
    ) {
        devices.removeIf(dev -> !dev.getDeviceInfo().getVendor().equals(vendorName)
                                || !dev.getDeviceInfo().getModel().equals(modelName));
    }

    private static void filterDevicesForType(
        final Collection<MockDevice> devices,
        final DeviceType devType
    ) {
        devices.removeIf(dev -> !dev.getDeviceStatus().getDeviceType().equals(devType));
    }

    private static void filterDevicesForVendor(
        final Collection<MockDevice> devices,
        final String vendorName
    ) {
        devices.removeIf(dev -> !dev.getDeviceInfo().getVendor().equals(vendorName));
    }

    private static Collection<MockDevice> getOrderedDevicesForMachine(
        final MockLiqidClient client,
        final DeviceType devType,
        final MockMachine machine
    ) throws LiqidException {
        var group = (MockGroup) client.getGroup(machine.getGroupId());

        TreeSet<MockDevice> thisMachineDevs = new TreeSet<>(machine.getAttachedDevices());
        TreeSet<MockDevice> freeDevs = new TreeSet<>(group.getFreePool());
        TreeSet<MockDevice> otherMachineDevIds = new TreeSet<>();
        for (var otherMachine : group.getMockMachines()) {
            if (!otherMachine.getMachineId().equals(machine.getMachineId())) {
                otherMachineDevIds.addAll(otherMachine.getAttachedDevices());
            }
        }

        var allDevs = new LinkedList<>(thisMachineDevs);
        allDevs.addAll(freeDevs);
        allDevs.addAll(otherMachineDevIds);
        filterDevicesForType(allDevs, devType);
        return allDevs;
    }

    private static void removeDevicesForModel(
        final Collection<MockDevice> devices,
        final String vendorName,
        final String modelName
    ) {
        devices.removeIf(dev -> dev.getDeviceInfo().getVendor().equals(vendorName)
                                && dev.getDeviceInfo().getModel().equals(modelName));
    }

    @Test
    public void getOrderedIdentifiers_SimpleSpecific() throws LiqidException {
        var mock = createMock();
        var inventory = LiqidInventory.createLiqidInventory(mock);

        var resModelL40 = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "L40");

        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resModelL40, Collections.emptyList(), 1);
        assertEquals(_l40Devs, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleVendor() throws LiqidException {
        var mock = createMock();
        var inventory = LiqidInventory.createLiqidInventory(mock);

        var resVendorNVidia = new VendorResourceModel(GeneralType.GPU, NVIDIA);

        var expList = new LinkedList<>(_a100Devs);
        expList.addAll(_l40Devs);

        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resVendorNVidia, Collections.emptyList(), 1);
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleGeneric() throws LiqidException {
        var mock = createMock();
        var inventory = LiqidInventory.createLiqidInventory(mock);

        var resGPU = new GenericResourceModel(GeneralType.GPU);

        var expList = new LinkedList<>(_a100Devs);
        expList.addAll(_l40Devs);
        expList.addAll(_a770Devs);

        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), 1);
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleGenericWithVendorRestrictions() throws LiqidException {
        var mock = createMock();
        var inventory = LiqidInventory.createLiqidInventory(mock);

        var resNVidia = new VendorResourceModel(GeneralType.GPU, NVIDIA);
        var resGPU = new GenericResourceModel(GeneralType.GPU);

        var restrictions = Arrays.asList(new ResourceModel[]{ resNVidia });
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, restrictions, 1);
        assertEquals(_a770Devs, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleGenericWithMultipleRestrictions() throws LiqidException {
        var mock = createMock();
        var inventory = LiqidInventory.createLiqidInventory(mock);

        var resIntel = new VendorResourceModel(GeneralType.GPU, INTEL);
        var resModelA100 = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        var resGPU = new GenericResourceModel(GeneralType.GPU);

        var restrictions = Arrays.asList(new ResourceModel[]{ resIntel, resModelA100 });
        var expected = new LinkedList<>(_l40Devs);

        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, restrictions, 1);
        assertEquals(expected, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleConflictingRestrictions() throws LiqidException {
        var mock = createMock();
        var inventory = LiqidInventory.createLiqidInventory(mock);

        var resNVidia = new VendorResourceModel(GeneralType.GPU, NVIDIA);
        var resIntel = new VendorResourceModel(GeneralType.GPU, INTEL);
        var resGPU = new GenericResourceModel(GeneralType.GPU);

        var expList = new LinkedList<Integer>();
        var restrictions = Arrays.asList(new ResourceModel[]{ resNVidia, resIntel });
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, restrictions, 1);
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiers_Complicated() throws LiqidException {
        var mock = createMock();

        // Everything goes into the group's free pool first...
        var groupId = _group.getGroupId();
        mock.groupPoolEdit(groupId);
        for (var devStat : mock.getAllDevicesStatus()) {
            mock.addDeviceToGroup(devStat.getDeviceId(), groupId);
        }
        mock.groupPoolDone(groupId);

        var a100List = new LinkedList<>(_a100Devs);
        var l40List = new LinkedList<>(_l40Devs);
        var a770List = new LinkedList<>(_a770Devs);

        // Arthur gets one of each type of GPU
        mock.editFabric(_arthurMachine.getMachineId());
        mock.addDeviceToMachine(a100List.pop(), _group.getGroupId(), _arthurMachine.getMachineId());
        mock.addDeviceToMachine(l40List.pop(), _group.getGroupId(), _arthurMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _arthurMachine.getMachineId());
        mock.reprogramFabric(_arthurMachine.getMachineId());

        //  Ford gets two A100s
        mock.editFabric(_fordMachine.getMachineId());
        mock.addDeviceToMachine(a100List.pop(), _group.getGroupId(), _fordMachine.getMachineId());
        mock.addDeviceToMachine(a100List.pop(), _group.getGroupId(), _fordMachine.getMachineId());
        mock.reprogramFabric(_fordMachine.getMachineId());

        //  Trillian gets three A770s.
        mock.editFabric(_trillianMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        mock.reprogramFabric(_trillianMachine.getMachineId());

        var inventory = LiqidInventory.createLiqidInventory(mock);

        // Now, we're going to get the ordered device identifiers for all the GPUs,
        // from the perspective of each of the machines.
        // Each machine wants to see their own GPUs first, then the unassigned GPUs, then all the rest of the GPUs.
        var resGPU = new GenericResourceModel(GeneralType.GPU);

        var expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _arthurMachine);
        var expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), _arthurMachine.getMachineId());
        assertEquals(expected, list);

        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _fordMachine);
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), _fordMachine.getMachineId());
        assertEquals(expected, list);

        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _trillianMachine);
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        list = Allocator.getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), _trillianMachine.getMachineId());
        assertEquals(expected, list);
    }

    @Test
    public void getOrderedIdentifiersOfVendor_Complicated() throws LiqidException {
        var mock = createMock();

        // Everything goes into the group's free pool first...
        var groupId = _group.getGroupId();
        mock.groupPoolEdit(groupId);
        for (var devStat : mock.getAllDevicesStatus()) {
            mock.addDeviceToGroup(devStat.getDeviceId(), groupId);
        }
        mock.groupPoolDone(groupId);

        var a100List = new LinkedList<>(_a100Devs);
        var l40List = new LinkedList<>(_l40Devs);
        var a770List = new LinkedList<>(_a770Devs);

        // Arthur gets one of each type of GPU
        mock.editFabric(_arthurMachine.getMachineId());
        mock.addDeviceToMachine(a100List.pop(), _group.getGroupId(), _arthurMachine.getMachineId());
        mock.addDeviceToMachine(l40List.pop(), _group.getGroupId(), _arthurMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _arthurMachine.getMachineId());
        mock.reprogramFabric(_arthurMachine.getMachineId());

        //  Ford gets two A100s
        mock.editFabric(_fordMachine.getMachineId());
        mock.addDeviceToMachine(a100List.pop(), _group.getGroupId(), _fordMachine.getMachineId());
        mock.addDeviceToMachine(a100List.pop(), _group.getGroupId(), _fordMachine.getMachineId());
        mock.reprogramFabric(_fordMachine.getMachineId());

        //  Trillian gets three A770s.
        mock.editFabric(_trillianMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        mock.reprogramFabric(_trillianMachine.getMachineId());

        var inventory = LiqidInventory.createLiqidInventory(mock);

        // Everyone wants all the NVidia GPUs, but the preference order varies depending on the machine.
        // Pretty much the same thing as above, but only for NVidia.
        var resNVidia = new VendorResourceModel(GeneralType.GPU, NVIDIA);

        var expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _arthurMachine);
        filterDevicesForVendor(expectedDevs, NVIDIA);
        var expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        var list = Allocator.getOrderedDeviceIdentifiers(inventory, resNVidia, Collections.emptyList(), _arthurMachine.getMachineId());
        assertEquals(expected, list);

        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _fordMachine);
        filterDevicesForVendor(expectedDevs, NVIDIA);
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        list = Allocator.getOrderedDeviceIdentifiers(inventory, resNVidia, Collections.emptyList(), _fordMachine.getMachineId());
        assertEquals(expected, list);

        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _trillianMachine);
        filterDevicesForVendor(expectedDevs, NVIDIA);
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        list = Allocator.getOrderedDeviceIdentifiers(inventory, resNVidia, Collections.emptyList(), _trillianMachine.getMachineId());
        assertEquals(expected, list);
    }

    @Test
    public void simpleAllocations_AllUnowned() throws LiqidException {
        var mock = createMock();
        // none of the devices are assigned to machines
        var inventory = LiqidInventory.createLiqidInventory(mock);

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

        var gpuDevs = mock.getFreeGPUDevicesStatus();
        var expected = MockLiqidClient.getDeviceIdsFromDeviceStatuses(gpuDevs);

        assertEquals(1, allocs.values().size());
        for (var entry : allocs.entrySet()) {
            var allocsForResModel = entry.getValue();
            assertEquals(3, allocsForResModel.size());
            for (var alloc : allocsForResModel) {
                assertEquals((Integer)2, alloc.getCount());
                assertEquals(expected, alloc.getDeviceIdentifiers());
            }
        }
    }

    @Test
    public void complicatedAllocations() throws LiqidException {
        var mock = createMock();

        // Everything goes into the group's free pool first...
        var groupId = _group.getGroupId();
        mock.groupPoolEdit(groupId);
        for (var devStat : mock.getAllDevicesStatus()) {
            mock.addDeviceToGroup(devStat.getDeviceId(), groupId);
        }
        mock.groupPoolDone(groupId);

        // Arthur gets two A100s
        // Ford gets two L40s
        // Trillian gets two L40s
        // Zaphod gets three A770s

        var a100List = new LinkedList<>(_a100Devs);
        var l40List = new LinkedList<>(_l40Devs);
        var a770List = new LinkedList<>(_a770Devs);

        mock.editFabric(_arthurMachine.getMachineId());
        mock.addDeviceToMachine(a100List.pop(), _group.getGroupId(), _arthurMachine.getMachineId());
        mock.addDeviceToMachine(a100List.pop(), _group.getGroupId(), _arthurMachine.getMachineId());
        mock.reprogramFabric(_arthurMachine.getMachineId());

        mock.editFabric(_fordMachine.getMachineId());
        mock.addDeviceToMachine(l40List.pop(), _group.getGroupId(), _fordMachine.getMachineId());
        mock.addDeviceToMachine(l40List.pop(), _group.getGroupId(), _fordMachine.getMachineId());
        mock.reprogramFabric(_fordMachine.getMachineId());

        mock.editFabric(_trillianMachine.getMachineId());
        mock.addDeviceToMachine(l40List.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        mock.addDeviceToMachine(l40List.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        mock.reprogramFabric(_trillianMachine.getMachineId());

        mock.editFabric(_zaphodMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _zaphodMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _zaphodMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _zaphodMachine.getMachineId());
        mock.reprogramFabric(_zaphodMachine.getMachineId());

        var inventory = LiqidInventory.createLiqidInventory(mock);

        // New layout:
        // Arthur will want 1 A100 and 2 L40's,
        // Ford 2 will want 4 Intels
        // Trillian will want an A100.
        // Zaphod will take 4 of anything except A100's
        var layout = new ClusterLayout();

        var machProf1 = new MachineProfile(inventory.getMachine(1));
        var resModel1a = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        var resModel1b = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "L40");
        machProf1.injectCount(resModel1a, 1);
        machProf1.injectCount(resModel1b, 2);

        var machProf2 = new MachineProfile(inventory.getMachine(2));
        var resModel2 = new VendorResourceModel(GeneralType.GPU, INTEL);
        machProf2.injectCount(resModel2, 4);

        var machProf3 = new MachineProfile(inventory.getMachine(3));
        var resModel3 = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        machProf3.injectCount(resModel3, 1);

        var machProf4 = new MachineProfile(inventory.getMachine(4));
        var resModel4a = new GenericResourceModel(GeneralType.GPU);
        var resModel4b = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        machProf4.injectCount(resModel4a, 4);
        machProf4.injectCount(resModel4b, 0);

        layout.addMachineProfile(machProf1);
        layout.addMachineProfile(machProf2);
        layout.addMachineProfile(machProf3);
        layout.addMachineProfile(machProf4);

        // finally <whew> we do the allocator thing.
        var allocator = new Allocator();
        allocator.populateAllocations(inventory, layout);
        var allocs = allocator.getAllocations();

        assertEquals(4, allocs.values().size());

        /* NVidia A100 - Arthur wants 1, Trillian wants 1 */
        var iter = allocs.entrySet().iterator();
        var entry = iter.next();
        var resModel = entry.getKey();
        var allocations = entry.getValue();
        System.out.printf("%s <=> %s\n", resModel, allocations);

        assertEquals(GeneralType.GPU, resModel.getGeneralType());
        assertEquals(NVIDIA, resModel.getVendorName());
        assertEquals("A100", resModel.getModelName());
        assertTrue(resModel instanceof SpecificResourceModel);
        assertEquals(2, allocations.size());

        var subIter = allocations.iterator();
        var alloc = subIter.next();
        assertEquals(ARTHUR, alloc.getMachine().getMachineName());
        assertEquals((Integer) 1, alloc.getCount());
        var expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _arthurMachine);
        filterDevicesForModel(expectedDevs, resModel.getVendorName(), resModel.getModelName());
        var expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        alloc = subIter.next();
        assertEquals(TRILLIAN, alloc.getMachine().getMachineName());
        assertEquals((Integer) 1, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _trillianMachine);
        filterDevicesForModel(expectedDevs, resModel.getVendorName(), resModel.getModelName());
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        /* NVidia L40 - Arthur wants 2 */
        entry = iter.next();
        resModel = entry.getKey();
        allocations = entry.getValue();
        System.out.printf("%s <=> %s\n", resModel, allocations);

        assertEquals(GeneralType.GPU, resModel.getGeneralType());
        assertEquals(NVIDIA, resModel.getVendorName());
        assertEquals("L40", resModel.getModelName());
        assertTrue(resModel instanceof SpecificResourceModel);
        assertEquals(1, allocations.size());

        subIter = allocations.iterator();
        alloc = subIter.next();
        assertEquals(ARTHUR, alloc.getMachine().getMachineName());
        assertEquals((Integer) 2, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _arthurMachine);
        filterDevicesForModel(expectedDevs, resModel.getVendorName(), resModel.getModelName());
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        /* Intel - Ford wants 4 */
        entry = iter.next();
        resModel = entry.getKey();
        allocations = entry.getValue();
        System.out.printf("%s <=> %s\n", resModel, allocations);

        assertEquals(GeneralType.GPU, resModel.getGeneralType());
        assertEquals(INTEL, resModel.getVendorName());
        assertTrue(resModel instanceof VendorResourceModel);
        assertEquals(1, allocations.size());

        subIter = allocations.iterator();
        alloc = subIter.next();
        assertEquals(FORD, alloc.getMachine().getMachineName());
        assertEquals((Integer) 4, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _fordMachine);
        filterDevicesForVendor(expectedDevs, resModel.getVendorName());
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
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
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _zaphodMachine);
        removeDevicesForModel(expectedDevs, NVIDIA, "A100");
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());
    }

    @Test
    public void complicatedAllocations_MultipleTypes() throws LiqidException {
        var mock = createMock();

        // Everything goes into the group's free pool first...
        var groupId = _group.getGroupId();
        mock.groupPoolEdit(groupId);
        for (var devStat : mock.getAllDevicesStatus()) {
            mock.addDeviceToGroup(devStat.getDeviceId(), groupId);
        }
        mock.groupPoolDone(groupId);

        // Starting point -
        //  Arthur gets two A100s
        //  Ford gets two L40s and all the bears
        //  Trillian gets two L40s and all the snakes
        //  Zaphod gets three A770s and all the FPGAs
        var a100List = new LinkedList<>(_a100Devs);
        var l40List = new LinkedList<>(_l40Devs);
        var a770List = new LinkedList<>(_a770Devs);
        var fpgaList = new LinkedList<>(_z5Devs);
        var bearList = new LinkedList<>(_bearDevs);
        var snakeList = new LinkedList<>(_snakeDevs);

        mock.editFabric(_arthurMachine.getMachineId());
        mock.addDeviceToMachine(a100List.pop(), _group.getGroupId(), _arthurMachine.getMachineId());
        mock.addDeviceToMachine(a100List.pop(), _group.getGroupId(), _arthurMachine.getMachineId());
        mock.reprogramFabric(_arthurMachine.getMachineId());

        mock.editFabric(_fordMachine.getMachineId());
        mock.addDeviceToMachine(l40List.pop(), _group.getGroupId(), _fordMachine.getMachineId());
        mock.addDeviceToMachine(l40List.pop(), _group.getGroupId(), _fordMachine.getMachineId());
        while (!bearList.isEmpty()) {
            mock.addDeviceToMachine(bearList.pop(), _group.getGroupId(), _fordMachine.getMachineId());
        }
        mock.reprogramFabric(_fordMachine.getMachineId());

        mock.editFabric(_trillianMachine.getMachineId());
        mock.addDeviceToMachine(l40List.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        mock.addDeviceToMachine(l40List.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        while (!snakeList.isEmpty()) {
            mock.addDeviceToMachine(snakeList.pop(), _group.getGroupId(), _trillianMachine.getMachineId());
        }
        mock.reprogramFabric(_trillianMachine.getMachineId());

        mock.editFabric(_zaphodMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _zaphodMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _zaphodMachine.getMachineId());
        mock.addDeviceToMachine(a770List.pop(), _group.getGroupId(), _zaphodMachine.getMachineId());
        while (!fpgaList.isEmpty()) {
            mock.addDeviceToMachine(fpgaList.pop(), _group.getGroupId(), _zaphodMachine.getMachineId());
        }
        mock.reprogramFabric(_zaphodMachine.getMachineId());

        // Now the new layout...
        var inventory = LiqidInventory.createLiqidInventory(mock);

        // Arthur will want 1 A100 and 2 L40's, and two SSDs of any type
        // Ford will want 4 Intels, and 2 SSDs which are *not* bears
        // Trillian will want an A100, and a bear and a snake and 10 FPGAs.
        // Zaphod will take 4 of any GPUs except A100's, and one SSD which is *not* a snake
        var layout = new ClusterLayout();

        /* Arthur */
        var machProf1 = new MachineProfile(inventory.getMachine(1));
        var resModel1a = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        var resModel1b = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "L40");
        var resModel1c = new GenericResourceModel(GeneralType.SSD);
        machProf1.injectCount(resModel1a, 1);
        machProf1.injectCount(resModel1b, 2);
        machProf1.injectCount(resModel1c, 2);

        /* Ford */
        var machProf2 = new MachineProfile(inventory.getMachine(2));
        var resModel2a = new VendorResourceModel(GeneralType.GPU, INTEL);
        var resModel2b = new GenericResourceModel(GeneralType.SSD);
        var resModel2c = new SpecificResourceModel(GeneralType.SSD, MICRON, "Bear");
        machProf2.injectCount(resModel2a, 4);
        machProf2.injectCount(resModel2b, 2);
        machProf2.injectCount(resModel2c, 0);

        /* Trillian */
        var machProf3 = new MachineProfile(inventory.getMachine(3));
        var resModel3a = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        var resModel3b = new SpecificResourceModel(GeneralType.SSD, MICRON, "Bear");
        var resModel3c = new SpecificResourceModel(GeneralType.SSD, MICRON, "Snake");
        var resModel3d = new GenericResourceModel(GeneralType.FPGA);
        machProf3.injectCount(resModel3a, 1);
        machProf3.injectCount(resModel3b, 1);
        machProf3.injectCount(resModel3c, 1);
        machProf3.injectCount(resModel3d, 10);

        /* Zaphod */
        var machProf4 = new MachineProfile(inventory.getMachine(4));
        var resModel4a = new GenericResourceModel(GeneralType.GPU);
        var resModel4b = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        var resModel4c = new GenericResourceModel(GeneralType.SSD);
        var resModel4d = new SpecificResourceModel(GeneralType.SSD, MICRON, "Snake");
        machProf4.injectCount(resModel4a, 4);
        machProf4.injectCount(resModel4b, 0);
        machProf4.injectCount(resModel4c, 1);
        machProf4.injectCount(resModel4d, 0);
        layout.addMachineProfile(machProf1);
        layout.addMachineProfile(machProf2);
        layout.addMachineProfile(machProf3);
        layout.addMachineProfile(machProf4);

        // Time to do the allocations, then test the results -------

        var allocator = new Allocator();
        allocator.populateAllocations(inventory, layout);
        var allocs = allocator.getAllocations();

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
        var expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.FPGA, _trillianMachine);
        var expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        /* NVidia A100 - Arthur wants 1, Trillian wants 1 */
        entry = iter.next();
        resModel = entry.getKey();
        allocations = entry.getValue();
        System.out.printf("%s <=> %s\n", resModel, allocations);

        assertEquals(GeneralType.GPU, resModel.getGeneralType());
        assertEquals(NVIDIA, resModel.getVendorName());
        assertEquals("A100", resModel.getModelName());
        assertTrue(resModel instanceof SpecificResourceModel);
        assertEquals(2, allocations.size());

        subIter = allocations.iterator();
        alloc = subIter.next();
        assertEquals(ARTHUR, alloc.getMachine().getMachineName());
        assertEquals((Integer) 1, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _arthurMachine);
        filterDevicesForModel(expectedDevs, NVIDIA, "A100");
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        alloc = subIter.next();
        assertEquals(TRILLIAN, alloc.getMachine().getMachineName());
        assertEquals((Integer) 1, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _trillianMachine);
        filterDevicesForModel(expectedDevs, NVIDIA, "A100");
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        /* NVidia L40 - Arthur wants 2 */
        entry = iter.next();
        resModel = entry.getKey();
        allocations = entry.getValue();
        System.out.printf("%s <=> %s\n", resModel, allocations);

        assertEquals(GeneralType.GPU, resModel.getGeneralType());
        assertEquals(NVIDIA, resModel.getVendorName());
        assertEquals("L40", resModel.getModelName());
        assertTrue(resModel instanceof SpecificResourceModel);
        assertEquals(1, allocations.size());

        subIter = allocations.iterator();
        alloc = subIter.next();
        assertEquals(ARTHUR, alloc.getMachine().getMachineName());
        assertEquals((Integer) 2, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _arthurMachine);
        filterDevicesForModel(expectedDevs, NVIDIA, "L40");
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        /* Intel - Ford wants 4 */
        entry = iter.next();
        resModel = entry.getKey();
        allocations = entry.getValue();
        System.out.printf("%s <=> %s\n", resModel, allocations);

        assertEquals(GeneralType.GPU, resModel.getGeneralType());
        assertEquals(INTEL, resModel.getVendorName());
        assertTrue(resModel instanceof VendorResourceModel);
        assertEquals(1, allocations.size());

        subIter = allocations.iterator();
        alloc = subIter.next();
        assertEquals(FORD, alloc.getMachine().getMachineName());
        assertEquals((Integer) 4, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _fordMachine);
        filterDevicesForVendor(expectedDevs, INTEL);
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
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
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _zaphodMachine);
        removeDevicesForModel(expectedDevs, NVIDIA, "A100");
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        /* Micron Bear SSD - Trillian wants just 1 */
        entry = iter.next();
        resModel = entry.getKey();
        allocations = entry.getValue();
        System.out.printf("%s <=> %s\n", resModel, allocations);

        assertEquals(GeneralType.SSD, resModel.getGeneralType());
        assertEquals(MICRON, resModel.getVendorName());
        assertEquals("Bear", resModel.getModelName());
        assertTrue(resModel instanceof SpecificResourceModel);
        assertEquals(1, allocations.size());

        subIter = allocations.iterator();
        alloc = subIter.next();
        assertEquals(TRILLIAN, alloc.getMachine().getMachineName());
        assertEquals((Integer) 1, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.SSD, _trillianMachine);
        filterDevicesForModel(expectedDevs, MICRON, "Bear");
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        /* Micron Snake SSD - Trillian wants 1 of these also */
        entry = iter.next();
        resModel = entry.getKey();
        allocations = entry.getValue();
        System.out.printf("%s <=> %s\n", resModel, allocations);

        assertEquals(GeneralType.SSD, resModel.getGeneralType());
        assertEquals(MICRON, resModel.getVendorName());
        assertEquals("Snake", resModel.getModelName());
        assertTrue(resModel instanceof SpecificResourceModel);
        assertEquals(1, allocations.size());

        subIter = allocations.iterator();
        alloc = subIter.next();
        assertEquals(TRILLIAN, alloc.getMachine().getMachineName());
        assertEquals((Integer) 1, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.SSD, _trillianMachine);
        filterDevicesForModel(expectedDevs, MICRON, "Snake");
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
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
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.SSD, _arthurMachine);
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        alloc = subIter.next();
        assertEquals(FORD, alloc.getMachine().getMachineName());
        assertEquals((Integer) 2, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.SSD, _arthurMachine);
        removeDevicesForModel(expectedDevs, MICRON, "Bear");
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());

        alloc = subIter.next();
        assertEquals(ZAPHOD, alloc.getMachine().getMachineName());
        assertEquals((Integer) 1, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.SSD, _arthurMachine);
        removeDevicesForModel(expectedDevs, MICRON, "Snake");
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());
    }
}
