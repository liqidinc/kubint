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
import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CommandTest extends Command {

    public CommandTest() {
        super(new Logger("Test"), false, 0);
    }

    @Test
    public void createMachines() {
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

    @Override
    public Plan process() {
        return null;
    }
}
