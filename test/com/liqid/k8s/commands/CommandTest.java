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
import com.liqid.k8s.layout.DeviceItem;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.AnnotateNode;
import com.liqid.sdk.ComputeDeviceStatus;
import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.DeviceType;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommandTest extends Command {

    public CommandTest() {
        super(new Logger("Test"), false, 0);
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
        allocateEqually(computeDeviceItems, resourceDeviceItems, plan);
        plan.show();

        var fpgaTally = 0;
        var fpgaPerLow = fpgaCount / nodeCount;
        var fpgaPerHigh = (fpgaCount % nodeCount == 0) ? fpgaPerLow : fpgaPerLow + 1;
        var gpuTally = 0;
        var gpuPerLow = gpuCount / nodeCount;
        var gpuPerHigh = (gpuCount % nodeCount == 0) ? gpuPerLow : gpuPerLow + 1;

        for (var action : plan.getActions()) {
            var annotateAction = (AnnotateNode) action;
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
        createMachines(plan, map);
        plan.show();

        assertEquals(12, plan.getActions().size());
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
