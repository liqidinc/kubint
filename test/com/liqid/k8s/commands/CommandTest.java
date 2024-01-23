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
import com.liqid.sdk.mock.MockLiqidClient;
import org.junit.Test;

import java.util.Arrays;
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
     * Helpful wrapper to create a full annotation key for a device-specific resource count.
     * @param genType the general type of interest
     * @return the full key for this type of resource counter (with the company prefix applied)
     */
    private static String createAnnotationKeyForDeviceType(
        final GeneralType genType
    ) {
        return createAnnotationKeyFor(ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType));
    }

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

        var plan = new Plan();
        var layout = createEvenlyAllocatedClusterLayout(computeDeviceItems, resourceDeviceItems);
        System.out.println("Plan");
        plan.show();
        System.out.println("Layout");
        layout.show("");

        var fpgaTally = 0;
        var fpgaPerLow = fpgaCount / nodeCount;
        var fpgaPerHigh = (fpgaCount % nodeCount == 0) ? fpgaPerLow : fpgaPerLow + 1;
        var gpuTally = 0;
        var gpuPerLow = gpuCount / nodeCount;
        var gpuPerHigh = (gpuCount % nodeCount == 0) ? gpuPerLow : gpuPerLow + 1;

        for (var action : plan.getActions()) {
            var annotateAction = (AnnotateNodeAction) action;
            for (var anno : annotateAction.getAnnotations().entrySet()) {
                var annoKey = anno.getKey();
                var annoValue = Integer.parseInt(anno.getValue());
                if (annoKey.contains("fpga")) {
                    fpgaTally += annoValue;
                    assertTrue(annoValue >= fpgaPerLow && annoValue <= fpgaPerHigh);
                } else if (annoKey.contains("gpu")) {
                    gpuTally += annoValue;
                    assertTrue(annoValue >= gpuPerLow && annoValue <= gpuPerHigh);
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
            assertTrue(action instanceof AnnotateNodeAction an);
            if (action instanceof AnnotateNodeAction an) {
                var annos = an.getAnnotations();
                if (an.getNodeName().equals("worker-1")) {
                    assertEquals(5, annos.size());
                    assertEquals("3", annos.get(fpgaKey));
                    assertNull(annos.get(linkKey));
                    assertEquals("NVidia:4,Intel:A770:0,Intel:4", annos.get(gpuKey));
                    assertEquals("Liqid:5", annos.get(ssdKey));
                } else if (an.getNodeName().equals("worker-2")) {
                    assertEquals(5, annos.size());
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

    @Override
    public Plan process() {
        return null;
    }
}
