/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan;

import com.liqid.k8s.Layout;
import com.liqid.k8s.LiqidConfiguration;
import com.liqid.k8s.MachineInfo;
import com.liqid.k8s.exceptions.ProcessingException;
import com.bearsnake.k8sclient.K8SClient;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;

public class Plan {

//    private final ArrayList<Step> _steps = new ArrayList<>();
//
//    private Plan(
//        final Collection<Step> steps
//    ) {
//        _steps.addAll(steps);
//    }
//
//    public Step getStep(final int index) { return _steps.get(index); }
//    public int getStepCount() { return _steps.size(); }
//
//    public static Plan createForAddingNodes(
//        final Layout currentLayout,
//        final Layout targetLayout
//    ) {
//        var steps = new LinkedList<Step>();
//
//        for (var mi : targetLayout._machineInfos.values()) {
//            if (currentLayout.getMachineInfo(mi.getMachineName()) == null) {
//                var machineName = mi.getMachineName();
//                var k8sNodeName = mi.getK8SNodeName();
//
//                var devNames = new LinkedList<String>();
//                devNames.add(mi.getComputeDeviceName());
//                devNames.addAll(mi.getDeviceNames());
//
//                steps.add(new CreateMachine(machineName));
//                steps.add(new AssignToMachine(machineName, k8sNodeName, devNames));
//            }
//        }
//
//        return new Plan(steps);
//    }
//
//    /**
//     * Creates a plan for clearing the liqid configuration and subsequently
//     * reaching the given requested layout.
//     */
//    public static Plan createForInitialConfiguration(
//        final LiqidConfiguration liqidConfig,
//        final boolean clearContext,
//        final String liqidK8SGroupName,
//        final Layout layout
//    ) {
//        var steps = new LinkedList<Step>();
//        if (clearContext) {
//            steps.add(new ClearConfiguration());
//        }
//
//        // Create the liqid group and then assign all the devices to that group.
//        steps.add(new CreateGroup(liqidK8SGroupName));
//        steps.add(new AssignToGroup(liqidK8SGroupName, liqidConfig.getDeviceNames()));
//
//        for (var mi : layout._machineInfos.values()) {
//            var machineName = mi.getMachineName();
//            var k8sNodeName = mi.getK8SNodeName();
//
//            var devNames = new LinkedList<String>();
//            devNames.add(mi.getComputeDeviceName());
//            devNames.addAll(mi.getDeviceNames());
//
//            steps.add(new CreateMachine(machineName));
//            steps.add(new AssignToMachine(machineName, k8sNodeName, devNames));
//        }
//
//        return new Plan(steps);
//    }
//
//    public static Plan createForReconfiguration(
//        final Layout currentLayout,
//        final Layout targetLayout
//    ) {
//        // This algorithm could be smarter.
//        // We could look closely at the movement of the devices among the machines in order to
//        // avoid taking a machine offline once to remove devices, and again to add devices.
//        // This is a non-trivial task, and we'll not do it for the first release.
//
//        var steps = new LinkedList<Step>();
//
//        // Sort machines into current ones we're removing, current ones we're updating,
//        // and new ones which are not current.
//        var addingMachines = new HashMap<String, MachineInfo>(); // target machines not in the current layout
//        var removingMachines = new HashMap<String, MachineInfo>(); // current machines not in the target layout
//        var losingDevices = new HashMap<String, Collection<String>>(); // machine names -> names of devices being lost
//        var gainingDevices = new HashMap<String, Collection<String>>(); // machine names -> names of devices being gained
//
//        for (var entry : currentLayout._machineInfos.entrySet()) {
//            var machName = entry.getKey();
//            var currentMachInfo = entry.getValue();
//            if (targetLayout._machineInfos.containsKey(machName)) {
//                var losingNames = new LinkedList<String>();
//                var gainingNames = new LinkedList<String>();
//                var targetMachInfo = targetLayout._machineInfos.get(machName);
//                for (var devName : currentMachInfo.getDeviceNames()) {
//                    if (!targetMachInfo.getDeviceNames().contains(devName)) {
//                        losingNames.add(devName);
//                    }
//                }
//                for (var devName : targetMachInfo.getDeviceNames()) {
//                    if (!currentMachInfo.getDeviceNames().contains(devName)) {
//                        gainingNames.add(devName);
//                    }
//                }
//
//                if (!losingNames.isEmpty()) {
//                    losingDevices.put(machName, losingNames);
//                }
//                if (!gainingNames.isEmpty()) {
//                    gainingDevices.put(machName, gainingNames);
//                }
//            } else {
//                removingMachines.put(machName, currentMachInfo);
//            }
//        }
//
//        for (var entry : targetLayout._machineInfos.entrySet()) {
//            var machName = entry.getKey();
//            var machInfo = entry.getValue();
//            if (!currentLayout._machineInfos.containsKey(machName)) {
//                addingMachines.put(machName, machInfo);
//                gainingDevices.put(machName, machInfo.getDeviceNames());
//            }
//        }
//
//        // First thing is to lose all the machines we're not needing any more.
//        for (var entry : removingMachines.entrySet()) {
//            var machName = entry.getKey();
//            steps.add(new DeleteMachine(machName));
//        }
//
//        // Create machines for the new mach infos.
//        for (var entry : addingMachines.entrySet()) {
//            var machName = entry.getKey();
//            steps.add(new CreateMachine(machName));
//        }
//
//        // Reconfigure the machines which are losing devices.
//        for (var entry : losingDevices.entrySet()) {
//            var machName = entry.getKey();
//            var devNames = entry.getValue();
//            var k8sNodeName = currentLayout._machineInfos.get(machName).getK8SNodeName();
//            steps.add(new RemoveFromMachine(machName, k8sNodeName, devNames));
//        }
//
//        // Reconfigure machines which are gaining devices.
//        for (var entry : gainingDevices.entrySet()) {
//            var machName = entry.getKey();
//            var devNames = entry.getValue();
//            var k8sNodeName = currentLayout._machineInfos.get(machName).getK8SNodeName();
//            steps.add(new AssignToMachine(machName, k8sNodeName, devNames));
//        }
//
//        return new Plan(steps);
//    }
//
//    public void execute(
//        final K8SClient k8SClient,
//        final LiqidClient liqidClient,
//        final Logger logger
//    ) throws ProcessingException {
//        execute(k8SClient, liqidClient, null, logger);
//    }
//
//    public void execute(
//        final K8SClient k8SClient,
//        final LiqidClient liqidClient,
//        final Group group,
//        final Logger logger
//    ) throws ProcessingException {
//        var context = new ExecutionContext(k8SClient, liqidClient, logger);
//        try {
//            context.loadMachines();
//            context.setGroup(group);
//        } catch (LiqidException lex) {
//            logger.catching(lex);
//            var t = new ProcessingException(lex);
//            logger.throwing(t);
//            throw t;
//        }
//
//        for (int sx = 0; sx < _steps.size(); ++sx) {
//            var step = _steps.get(sx);
//            System.out.printf("Executing Step %d: %s...\n", sx + 1, step.toString());
//            step.perform(context);
//        }
//    }
//
//    public void show() {
//        System.out.println();
//        System.out.println("Plan----------------------------------");
//        if (_steps.isEmpty()) {
//            System.out.println("Nothing to be done");
//        } else {
//            for (int sx = 0; sx < _steps.size(); ++sx) {
//                System.out.printf("| Step %d: %s\n", sx + 1, _steps.get(sx).toString());
//            }
//        }
//    }
}
