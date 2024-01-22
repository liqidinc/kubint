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
import com.liqid.sdk.ComputeDeviceStatus;
import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class CommandTest extends Command {

    public CommandTest() {
        super(new Logger("Test"), false, 0);
    }

    @Test
    public void allocateEqually_test() {
        var computeDeviceItems = new HashMap<DeviceItem, Node>();
        for (var cx = 0; cx < 3; cx++) {
            var devStat = new DeviceStatus();
            var devInfo = new DeviceInfo();
            var devItem = new DeviceItem(devStat, devInfo);
            var node = new Node();
            computeDeviceItems.put(devItem, node);
        }

        var resourceDeviceItems = new LinkedList<DeviceItem>();

        var plan = new Plan();
        allocateEqually(computeDeviceItems, resourceDeviceItems, plan);
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
