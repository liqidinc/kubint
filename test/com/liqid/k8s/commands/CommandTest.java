/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.Node;
import com.bearsnake.k8sclient.NodeMetadata;
import com.bearsnake.k8sclient.NodeSpec;
import com.bearsnake.k8sclient.NodeStatus;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Constants;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.layout.Allocator;
import com.liqid.k8s.layout.ClusterLayout;
import com.liqid.k8s.layout.DeviceItem;
import com.liqid.k8s.layout.GeneralType;
import com.liqid.k8s.layout.GenericResourceModel;
import com.liqid.k8s.layout.LiqidInventory;
import com.liqid.k8s.layout.MachineProfile;
import com.liqid.k8s.layout.ResourceModel;
import com.liqid.k8s.layout.SpecificResourceModel;
import com.liqid.k8s.layout.VendorResourceModel;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.AnnotateNodeAction;
import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CommandTest extends Command {

    public CommandTest() {
        super(new Logger("Test"), false, 0);
    }

    /**
     * Necessary due to base class abstractness
     */
    @Override
    public Plan process() {
        return null;
    }

    // Static data for MockLiqidClient
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

    private static String createAnnotationKeyForDeviceType(
        final GeneralType genType
    ) {
        return createAnnotationKeyFor(ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType));
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

    // -----------------------------------------------------------------------------------------------------------------------------

    @Test
    public void allocateEqually_test() {
        var computeDeviceItems = new HashMap<DeviceItem, Node>();
        var resourceDeviceItems = new LinkedList<DeviceItem>();

        var nodeCount = 3;
        for (var cx = 0; cx < nodeCount; cx++) {
            var devName = String.format("pcpu%d", cx);
            var nodeName = String.format("worker-%d", cx + 1);
            var devStat = new DeviceStatus().setDeviceId(cx).setName(devName).setDeviceType(DeviceType.COMPUTE);
            var devInfo = new DeviceInfo().setDeviceIdentifier(cx).setName(devName).setDeviceInfoType(DeviceType.COMPUTE);
            var devItem = new DeviceItem(devStat, devInfo);

            var nodeMetadata = new NodeMetadata().setName(nodeName);
            var nodeSpec = new NodeSpec();
            var nodeStatus = new NodeStatus();
            var node = new Node(nodeMetadata, nodeSpec, nodeStatus);

            computeDeviceItems.put(devItem, node);
        }

        var fpgaCount = 2;
        for (var dx = 0; dx < fpgaCount; dx++) {
            var devName = String.format("fpga%d", dx);
            var devId = 0x1B00 + dx;
            var devStat = new DeviceStatus().setDeviceId(devId).setName(devName).setDeviceType(DeviceType.FPGA);
            var devInfo = new DeviceInfo().setDeviceIdentifier(devId).setName(devName).setDeviceInfoType(DeviceType.FPGA);
            var devItem = new DeviceItem(devStat, devInfo);
            resourceDeviceItems.add(devItem);
        }

        var gpuCount = 13;
        for (var dx = 0; dx < gpuCount; dx++) {
            var devName = String.format("gpu%d", dx);
            var devId = 0xB00 + dx;
            var devStat = new DeviceStatus().setDeviceId(devId).setName(devName).setDeviceType(DeviceType.GPU);
            var devInfo = new DeviceInfo().setDeviceIdentifier(devId).setName(devName).setDeviceInfoType(DeviceType.GPU);
            var devItem = new DeviceItem(devStat, devInfo);
            resourceDeviceItems.add(devItem);
        }

        var layout = createEvenlyAllocatedClusterLayout(computeDeviceItems, resourceDeviceItems);
        System.out.println("Layout");
        layout.show("");

        var fpgaTally = 0;
        var fpgaPerLow = fpgaCount / nodeCount;
        var fpgaPerHigh = (fpgaCount % nodeCount == 0) ? fpgaPerLow : fpgaPerLow + 1;
        var gpuTally = 0;
        var gpuPerLow = gpuCount / nodeCount;
        var gpuPerHigh = (gpuCount % nodeCount == 0) ? gpuPerLow : gpuPerLow + 1;

        for (var profile : layout.getMachineProfiles()) {
            var resModels = profile.getResourceModels();
            for (var resModel : resModels) {
                var count = profile.getCount(resModel);
                if (resModel.getGeneralType() == GeneralType.GPU) {
                    assertTrue(count >= gpuPerLow && count <= gpuPerHigh);
                    gpuTally += count;
                } else if (resModel.getGeneralType() == GeneralType.FPGA) {
                    assertTrue(count >= fpgaPerLow && count <= fpgaPerHigh);
                    fpgaTally += count;
                }
            }
        }

        assertEquals(fpgaCount, fpgaTally);
        assertEquals(gpuCount, gpuTally);
    }

    @Test
    public void createAnnotationsFromClusterLayout_test() throws InternalErrorException {
        var nodes = new LinkedList<Node>();
        var layout = new ClusterLayout();
        var plan = new Plan();

        var annos1 = new HashMap<String, String>();
        annos1.put(createAnnotationKeyFor(Constants.K8S_ANNOTATION_MACHINE_NAME), "Machine1");
        var node1 = new Node(new NodeMetadata().setAnnotations(annos1).setName("worker-1"), new NodeSpec(), new NodeStatus());
        nodes.add(node1);

        var annos2 = new HashMap<String, String>();
        annos2.put(createAnnotationKeyFor(Constants.K8S_ANNOTATION_MACHINE_NAME), "Machine2");
        var node2 = new Node(new NodeMetadata().setAnnotations(annos2).setName("worker-2"), new NodeSpec(), new NodeStatus());
        nodes.add(node2);

        var profile1 = new MachineProfile("Machine1");
        profile1.injectCount(new GenericResourceModel(GeneralType.FPGA), 3);
        profile1.injectCount(new VendorResourceModel(GeneralType.SSD, "Liqid"), 5);
        profile1.injectCount(new VendorResourceModel(GeneralType.GPU, "NVidia"), 4);
        profile1.injectCount(new VendorResourceModel(GeneralType.GPU, "Intel"), 4);
        profile1.injectCount(new SpecificResourceModel(GeneralType.GPU, "Intel", "A770"), 0);
        layout.addMachineProfile(profile1);

        createAnnotationsFromClusterLayout(nodes, layout, plan);
        plan.show();

        var fpgaKey = ANNOTATION_KEY_FOR_DEVICE_TYPE.get(GeneralType.FPGA);
        var gpuKey = ANNOTATION_KEY_FOR_DEVICE_TYPE.get(GeneralType.GPU);
        var linkKey = ANNOTATION_KEY_FOR_DEVICE_TYPE.get(GeneralType.LINK);
        var memKey = ANNOTATION_KEY_FOR_DEVICE_TYPE.get(GeneralType.MEMORY);
        var ssdKey = ANNOTATION_KEY_FOR_DEVICE_TYPE.get(GeneralType.SSD);

        for (var action : plan.getActions()) {
            assertTrue(action instanceof AnnotateNodeAction);
            if (action instanceof AnnotateNodeAction an) {
                var annos = an.getAnnotations();
                if (an.getNodeName().equals("worker-1")) {
                    assertEquals(3, annos.size());
                    assertEquals("3", annos.get(fpgaKey));
                    assertNull(annos.get(linkKey));
                    assertTrue(annos.get(gpuKey).contains("NVidia:4"));
                    assertTrue(annos.get(gpuKey).contains("Intel:A770:0"));
                    assertTrue(annos.get(gpuKey).contains("Intel:4"));
                    assertEquals("Liqid:5", annos.get(ssdKey));
                } else if (an.getNodeName().equals("worker-2")) {
                    assertEquals(0, annos.size());
                    assertNull(annos.get(fpgaKey));
                    assertNull(annos.get(gpuKey));
                    assertNull(annos.get(linkKey));
                    assertNull(annos.get(memKey));
                    assertNull(annos.get(ssdKey));
                }
            }
        }
    }

    @Test
    public void createClusterLayoutFromAnnotations_test() throws LiqidException {
        _force = true;

        var liqidClient = new MockLiqidClient.Builder().build();
        var group = liqidClient.createGroup("Group");

        var machine1 = liqidClient.createMachine(group.getGroupId(), "machine1");
        var machine2 = liqidClient.createMachine(group.getGroupId(), "machine2");
        var machine3 = liqidClient.createMachine(group.getGroupId(), "machine3");

        var node1 = new Node(new NodeMetadata().setName("worker-1"), new NodeSpec(), new NodeStatus());
        var node2 = new Node(new NodeMetadata().setName("worker-2"), new NodeSpec(), new NodeStatus());
        var node3 = new Node(new NodeMetadata().setName("worker-3"), new NodeSpec(), new NodeStatus());
        var nodes = Arrays.asList(new Node[]{node1, node2, node3});

        nodes.get(0).metadata.annotations = new HashMap<>();
        nodes.get(0).metadata.annotations.put(createAnnotationKeyFor(Constants.K8S_ANNOTATION_MACHINE_NAME), "machine1");
        nodes.get(0).metadata.annotations.put(createAnnotationKeyForDeviceType(GeneralType.GPU), "NVidia:A100:3,Intel:A770:5,AMD:2,10");
        nodes.get(0).metadata.annotations.put(createAnnotationKeyForDeviceType(GeneralType.FPGA), "2");
        nodes.get(0).metadata.annotations.put(createAnnotationKeyForDeviceType(GeneralType.SSD), "Liqid:7");

        nodes.get(1).metadata.annotations = new HashMap<>();
        nodes.get(1).metadata.annotations.put(createAnnotationKeyFor(Constants.K8S_ANNOTATION_MACHINE_NAME), "machine2");
        nodes.get(1).metadata.annotations.put(createAnnotationKeyForDeviceType(GeneralType.GPU), "NVidia:5,NVidia:A100:0");

        nodes.get(2).metadata.annotations = new HashMap<>();
        nodes.get(2).metadata.annotations.put(createAnnotationKeyFor(Constants.K8S_ANNOTATION_MACHINE_NAME), "machine3");

        _liqidInventory = LiqidInventory.createLiqidInventory(liqidClient);
        var layout = createClusterLayoutFromAnnotations(nodes);
        assertNotNull(layout);
        layout.show("");

        var machProfile1 = layout.getMachineProfile(machine1.getMachineName());
        assertEquals(6, machProfile1.getResourceModels().size());

        ResourceModel resModel = new SpecificResourceModel(GeneralType.GPU, "NVidia", "A100");
        assertTrue(machProfile1.getResourceModels().contains(resModel));
        assertEquals((Integer)3, machProfile1.getCount(resModel));

        resModel = new SpecificResourceModel(GeneralType.GPU, "Intel", "A770");
        assertTrue(machProfile1.getResourceModels().contains(resModel));
        assertEquals((Integer)5, machProfile1.getCount(resModel));

        resModel = new VendorResourceModel(GeneralType.GPU, "AMD");
        assertTrue(machProfile1.getResourceModels().contains(resModel));
        assertEquals((Integer)2, machProfile1.getCount(resModel));

        resModel = new GenericResourceModel(GeneralType.GPU);
        assertTrue(machProfile1.getResourceModels().contains(resModel));
        assertEquals((Integer)10, machProfile1.getCount(resModel));

        resModel = new GenericResourceModel(GeneralType.FPGA);
        assertTrue(machProfile1.getResourceModels().contains(resModel));
        assertEquals((Integer)2, machProfile1.getCount(resModel));

        resModel = new VendorResourceModel(GeneralType.SSD, "Liqid");
        assertTrue(machProfile1.getResourceModels().contains(resModel));
        assertEquals((Integer)7, machProfile1.getCount(resModel));

        var machProfile2 = layout.getMachineProfile(machine2.getMachineName());
        assertEquals(2, machProfile2.getResourceModels().size());

        resModel = new VendorResourceModel(GeneralType.GPU, "NVidia");
        assertTrue(machProfile2.getResourceModels().contains(resModel));
        assertEquals((Integer)5, machProfile2.getCount(resModel));

        resModel = new SpecificResourceModel(GeneralType.GPU, "NVidia", "A100");
        assertTrue(machProfile2.getResourceModels().contains(resModel));
        assertEquals((Integer)0, machProfile2.getCount(resModel));

        var machProfile3 = layout.getMachineProfile(machine3.getMachineName());
        assertNull(machProfile3);
    }

    @Test
    public void createMachines_test() {
        DeviceStatus[] statuses = new DeviceStatus[3];
        DeviceInfo[] infos = new DeviceInfo[3];
        DeviceItem[] items = new DeviceItem[3];
        for (int dx = 0; dx < 3; dx++) {
            var name = String.format("pcpu%d", dx);
            statuses[dx] = new DeviceStatus().setName(name).setDeviceId(dx);
            infos[dx] = new DeviceInfo().setName(name).setDeviceIdentifier(dx);
            items[dx] = new DeviceItem(statuses[dx], infos[dx]);
        }

        Node[] nodes = new Node[3];
        for (int nx = 0; nx < 3; nx++) {
            var metadata = new NodeMetadata().setName(String.format("worker%d", nx));
            var spec = new NodeSpec();
            var status = new NodeStatus();
            nodes[nx] = new Node(metadata, spec, status);
        }

        Map<DeviceItem, Node> map = new HashMap<>();
        map.put(items[1], nodes[1]);
        map.put(items[2], nodes[2]);
        map.put(items[0], nodes[0]);

        var plan = new Plan();
        createMachines(map, plan);
        plan.show();

        assertEquals(12, plan.getActions().size());
        for (var entry : map.entrySet()) {
            var devItem = entry.getKey();
            var node = entry.getValue();
            assertEquals(node.getName(), devItem.getDeviceInfo().getUserDescription());
            var machName = createMachineName(devItem.getDeviceStatus(), node);
            var annoKey = createAnnotationKeyFor(Constants.K8S_ANNOTATION_MACHINE_NAME);
            assertEquals(machName, node.metadata.annotations.get(annoKey));
        }
    }

    @Test
    public void getIntersection_List() {
        var list1 = Arrays.asList(new Integer[]{ 1, 3, 2, 5, 5, 9, 10, 12, 5 });
        var list2 = Arrays.asList(new Integer[]{ 1, 5, 7, 7, 9, 9, 12, 5 });
        var list3 = new LinkedList<Integer>();
        var expected = Arrays.asList(new Integer[]{ 1, 5, 5, 9, 12 });
        getIntersection(list1, list2, list3);
        assertEquals(expected, list3);
    }

    @Test
    public void getIntersection_Set() {
        var list1 = Arrays.asList(new Integer[]{ 1, 3, 2, 5, 5, 9, 10, 12, 5 });
        var list2 = Arrays.asList(new Integer[]{ 1, 5, 7, 7, 9, 9, 12, 5 });
        var list3 = new TreeSet<Integer>();
        var expected = Arrays.asList(new Integer[]{ 1, 5, 5, 9, 12 });
        getIntersection(new TreeSet<>(list1), new TreeSet<>(list2), list3);
        assertEquals(new TreeSet<>(expected), list3);
    }

    @Test
    public void getOrderedIdentifiers_SimpleSpecific() throws LiqidException {
        var mock = createMock();
        var inventory = LiqidInventory.createLiqidInventory(mock);

        var resModelL40 = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "L40");

        var list = getOrderedDeviceIdentifiers(inventory, resModelL40, Collections.emptyList(), "");
        assertEquals(_l40Devs, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleVendor() throws LiqidException {
        var mock = createMock();
        var inventory = LiqidInventory.createLiqidInventory(mock);

        var resVendorNVidia = new VendorResourceModel(GeneralType.GPU, NVIDIA);

        var expList = new LinkedList<>(_a100Devs);
        expList.addAll(_l40Devs);

        var list = getOrderedDeviceIdentifiers(inventory, resVendorNVidia, Collections.emptyList(), "");
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

        var list = getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), "");
        assertEquals(expList, list);
    }

    @Test
    public void getOrderedIdentifiers_SimpleGenericWithVendorRestrictions() throws LiqidException {
        var mock = createMock();
        var inventory = LiqidInventory.createLiqidInventory(mock);

        var resNVidia = new VendorResourceModel(GeneralType.GPU, NVIDIA);
        var resGPU = new GenericResourceModel(GeneralType.GPU);

        var restrictions = Arrays.asList(new ResourceModel[]{ resNVidia });
        var list = getOrderedDeviceIdentifiers(inventory, resGPU, restrictions, "");
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

        var list = getOrderedDeviceIdentifiers(inventory, resGPU, restrictions, "");
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
        var list = getOrderedDeviceIdentifiers(inventory, resGPU, restrictions, "");
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
        var list = getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), ARTHUR);
        assertEquals(expected, list);

        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _fordMachine);
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        list = getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), FORD);
        assertEquals(expected, list);

        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _trillianMachine);
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        list = getOrderedDeviceIdentifiers(inventory, resGPU, Collections.emptyList(), TRILLIAN);
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
        var list = getOrderedDeviceIdentifiers(inventory, resNVidia, Collections.emptyList(), ARTHUR);
        assertEquals(expected, list);

        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _fordMachine);
        filterDevicesForVendor(expectedDevs, NVIDIA);
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        list = getOrderedDeviceIdentifiers(inventory, resNVidia, Collections.emptyList(), FORD);
        assertEquals(expected, list);

        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _trillianMachine);
        filterDevicesForVendor(expectedDevs, NVIDIA);
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        list = getOrderedDeviceIdentifiers(inventory, resNVidia, Collections.emptyList(), TRILLIAN);
        assertEquals(expected, list);
    }

    @Test
    public void createAllocators_allUnowned() throws LiqidException {
        var mock = createMock();
        // none of the devices are assigned to machines
        var inventory = LiqidInventory.createLiqidInventory(mock);

        // each machine wants 2 GPUs of any vendor and model.
        var layout = new ClusterLayout();
        var resModel = new GenericResourceModel(GeneralType.GPU);
        var machProf1 = new MachineProfile(inventory.getMachine(1).getMachineName());
        var machProf2 = new MachineProfile(inventory.getMachine(2).getMachineName());
        var machProf3 = new MachineProfile(inventory.getMachine(3).getMachineName());
        machProf1.injectCount(resModel, 2);
        machProf2.injectCount(resModel, 2);
        machProf3.injectCount(resModel, 2);
        layout.addMachineProfile(machProf1);
        layout.addMachineProfile(machProf2);
        layout.addMachineProfile(machProf3);

        var allocators = createAllocators(inventory, layout);

        var gpuDevs = mock.getFreeGPUDevicesStatus();
        var expected = MockLiqidClient.getDeviceIdsFromDeviceStatuses(gpuDevs);

        assertEquals(1, allocators.values().size());
        for (var entry : allocators.entrySet()) {
            var allocsForResModel = entry.getValue();
            assertEquals(3, allocsForResModel.size());
            for (var alloc : allocsForResModel) {
                assertEquals((Integer)2, alloc.getCount());
                assertEquals(expected, alloc.getDeviceIdentifiers());
            }
        }
    }

    @Test
    public void createAllocators_complicated() throws LiqidException {
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

        var machProf1 = new MachineProfile(ARTHUR);
        var resModel1a = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        var resModel1b = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "L40");
        machProf1.injectCount(resModel1a, 1);
        machProf1.injectCount(resModel1b, 2);

        var machProf2 = new MachineProfile(FORD);
        var resModel2 = new VendorResourceModel(GeneralType.GPU, INTEL);
        machProf2.injectCount(resModel2, 4);

        var machProf3 = new MachineProfile(TRILLIAN);
        var resModel3 = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        machProf3.injectCount(resModel3, 1);

        var machProf4 = new MachineProfile(ZAPHOD);
        var resModel4a = new GenericResourceModel(GeneralType.GPU);
        var resModel4b = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        machProf4.injectCount(resModel4a, 4);
        machProf4.injectCount(resModel4b, 0);

        layout.addMachineProfile(machProf1);
        layout.addMachineProfile(machProf2);
        layout.addMachineProfile(machProf3);
        layout.addMachineProfile(machProf4);

        // finally <whew> we do the allocator thing.
        var allocators = createAllocators(inventory, layout);

        assertEquals(4, allocators.values().size());

        /* NVidia A100 - Arthur wants 1, Trillian wants 1 */
        var iter = allocators.entrySet().iterator();
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
        while (subIter.hasNext()) {
            var alloc = subIter.next();
            if (alloc.getMachineName().equals(ARTHUR)) {
                assertEquals((Integer) 1, alloc.getCount());
                var expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _arthurMachine);
                filterDevicesForModel(expectedDevs, resModel.getVendorName(), resModel.getModelName());
                var expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
                assertEquals(expected, alloc.getDeviceIdentifiers());
            } else if (alloc.getMachineName().equals(TRILLIAN)) {
                assertEquals((Integer) 1, alloc.getCount());
                var expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _trillianMachine);
                filterDevicesForModel(expectedDevs, resModel.getVendorName(), resModel.getModelName());
                var expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
                assertEquals(expected, alloc.getDeviceIdentifiers());
            }
        }

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
        var alloc = subIter.next();
        assertEquals(ARTHUR, alloc.getMachineName());
        assertEquals((Integer) 2, alloc.getCount());
        var expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _arthurMachine);
        filterDevicesForModel(expectedDevs, resModel.getVendorName(), resModel.getModelName());
        var expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
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
        assertEquals(FORD, alloc.getMachineName());
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
        assertEquals(ZAPHOD, alloc.getMachineName());
        assertEquals((Integer) 4, alloc.getCount());
        expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _zaphodMachine);
        removeDevicesForModel(expectedDevs, NVIDIA, "A100");
        expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
        assertEquals(expected, alloc.getDeviceIdentifiers());
    }

    @Test
    public void createAllocators_MultipleTypes() throws LiqidException {
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
        var machProf1 = new MachineProfile(ARTHUR);
        var resModel1a = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        var resModel1b = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "L40");
        var resModel1c = new GenericResourceModel(GeneralType.SSD);
        machProf1.injectCount(resModel1a, 1);
        machProf1.injectCount(resModel1b, 2);
        machProf1.injectCount(resModel1c, 2);

        /* Ford */
        var machProf2 = new MachineProfile(FORD);
        var resModel2a = new VendorResourceModel(GeneralType.GPU, INTEL);
        var resModel2b = new GenericResourceModel(GeneralType.SSD);
        var resModel2c = new SpecificResourceModel(GeneralType.SSD, MICRON, "Bear");
        machProf2.injectCount(resModel2a, 4);
        machProf2.injectCount(resModel2b, 2);
        machProf2.injectCount(resModel2c, 0);

        /* Trillian */
        var machProf3 = new MachineProfile(TRILLIAN);
        var resModel3a = new SpecificResourceModel(GeneralType.GPU, NVIDIA, "A100");
        var resModel3b = new SpecificResourceModel(GeneralType.SSD, MICRON, "Bear");
        var resModel3c = new SpecificResourceModel(GeneralType.SSD, MICRON, "Snake");
        var resModel3d = new GenericResourceModel(GeneralType.FPGA);
        machProf3.injectCount(resModel3a, 1);
        machProf3.injectCount(resModel3b, 1);
        machProf3.injectCount(resModel3c, 1);
        machProf3.injectCount(resModel3d, 10);

        /* Zaphod */
        var machProf4 = new MachineProfile(ZAPHOD);
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

        var allocators = createAllocators(inventory, layout);

        assertEquals(8, allocators.values().size());

        /* Any FPGA - Trillian wants 10 of them */
        var iter = allocators.entrySet().iterator();
        var entry = iter.next();
        var resModel = entry.getKey();
        var allocations = entry.getValue();
        System.out.printf("%s <=> %s\n", resModel, allocations);

        assertEquals(GeneralType.FPGA, resModel.getGeneralType());
        assertTrue(resModel instanceof GenericResourceModel);
        assertEquals(1, allocations.size());

        var subIter = allocations.iterator();
        var alloc = subIter.next();
        assertEquals(TRILLIAN, alloc.getMachineName());
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
        while (subIter.hasNext()) {
            alloc = subIter.next();
            if (alloc.getMachineName().equals(ARTHUR)) {
                assertEquals((Integer) 1, alloc.getCount());
                expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _arthurMachine);
                filterDevicesForModel(expectedDevs, NVIDIA, "A100");
                expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
                assertEquals(expected, alloc.getDeviceIdentifiers());
            } else if (alloc.getMachineName().equals(TRILLIAN)) {
                assertEquals((Integer) 1, alloc.getCount());
                expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.GPU, _trillianMachine);
                filterDevicesForModel(expectedDevs, NVIDIA, "A100");
                expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
                assertEquals(expected, alloc.getDeviceIdentifiers());
            }
        }

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
        assertEquals(ARTHUR, alloc.getMachineName());
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
        assertEquals(FORD, alloc.getMachineName());
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
        assertEquals(ZAPHOD, alloc.getMachineName());
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
        assertEquals(TRILLIAN, alloc.getMachineName());
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
        assertEquals(TRILLIAN, alloc.getMachineName());
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
        while (subIter.hasNext()) {
            alloc = subIter.next();
            switch (alloc.getMachineName()) {
                case ARTHUR -> {
                    assertEquals((Integer) 2, alloc.getCount());
                    expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.SSD, _arthurMachine);
                    expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
                    assertEquals(expected, alloc.getDeviceIdentifiers());
                }
                case FORD -> {
                    assertEquals((Integer) 2, alloc.getCount());
                    expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.SSD, _arthurMachine);
                    removeDevicesForModel(expectedDevs, MICRON, "Bear");
                    expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
                    assertEquals(expected, alloc.getDeviceIdentifiers());
                }
                case ZAPHOD -> {
                    assertEquals((Integer) 1, alloc.getCount());
                    expectedDevs = getOrderedDevicesForMachine(mock, DeviceType.SSD, _arthurMachine);
                    removeDevicesForModel(expectedDevs, MICRON, "Snake");
                    expected = MockLiqidClient.getDeviceIdsFromMockDevices(expectedDevs);
                    assertEquals(expected, alloc.getDeviceIdentifiers());
                }
            }
        }
    }

    @Test
    public void createAllocations_test() {
        Map<ResourceModel, Collection<Allocator>> allocators = new HashMap<>();
        var gpuModel = new GenericResourceModel(GeneralType.GPU);
        // assume machine1 has 1, 2, 3 and machine2 has 4, 5, and free list has 6 through 13
        var alloc1 = new Allocator("Machine1", 5, Arrays.asList(new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}));
        var alloc2 = new Allocator("Machine2", 3, Arrays.asList(new Integer[]{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 1, 2, 3}));
        var alloc3 = new Allocator("Machine3", 3, Arrays.asList(new Integer[]{6, 7, 8, 9, 10, 11, 12, 13, 1, 2, 3, 4, 5}));
        allocators.put(gpuModel, Arrays.asList(new Allocator[]{ alloc1, alloc2, alloc3 }));

        var allocations = createAllocations(allocators);
        System.out.println(allocations);

        assertEquals(3, allocations.size());
        for (var alloc : allocations) {
            var machName = alloc.getMachineName();
            switch (machName) {
                case "Machine1" -> assertEquals(new TreeSet<>(Arrays.asList(new Integer[]{ 1, 2, 3, 4, 5 })), new TreeSet<>(alloc.getDeviceIdentifiers()));
                case "Machine2" -> assertEquals(new TreeSet<>(Arrays.asList(new Integer[]{ 6, 7, 8 })), new TreeSet<>(alloc.getDeviceIdentifiers()));
                case "Machine3" -> assertEquals(new TreeSet<>(Arrays.asList(new Integer[]{ 9, 10, 11 })), new TreeSet<>(alloc.getDeviceIdentifiers()));
            }
        }
    }
}
