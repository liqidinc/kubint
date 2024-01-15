/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.LiqidException;

import java.util.Collection;

public class ReleaseCommand extends Command {

    private Collection<String> _resourceSpecs;

    public ReleaseCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public ReleaseCommand setProxyURL(final String value) {_proxyURL = value; return this; }
    public ReleaseCommand setResourceSpecs(final Collection<String> list) { _resourceSpecs = list; return this; }

//    private Plan createPlan() {
//        var fn = this.getClass().getName() + ":createPlan";
//        _logger.trace("Entering %s", fn);
//
//        var plan = new Plan();
//
//        var groupActions = new HashMap<String, RemoveFromGroup>();
//        var machActions = new HashMap<String, Action>();
//        for (var devName : _resourceSpecs) {
//            var ds = _liqidInventory._deviceStatusByName.get(devName);
//            if (ds == null) {
//                System.err.printf("WARNING:Resource '%s' does not exist in the Liqid Cluster\n", devName);
//                continue;
//            }
//
//            var genType = LiqidGeneralType.fromDeviceType(ds.getDeviceType());
//            var group = _liqidInventory._groupsByDeviceId.get(ds.getDeviceId());
//            var groupName = (group == null) ? null : group.getGroupName();
//            var machine = _liqidInventory._machinesByDeviceId.get(ds.getDeviceId());
//            var machineName = (machine == null) ? null : machine.getMachineName();
//
//            if (genType == LiqidGeneralType.CPU) {
//                // Grab user description from the compute node, use that to generate an action
//                // to remove the annotation from the corresponding worker node.
//                // Also clear out the user description.
//                var nodeName = _liqidInventory.getK8sNodeNameFromComputeDevice(ds);
//                if (nodeName != null) {
//                    plan.addAction(new RemoveAnnotations().addNodeName(nodeName));
//                    plan.addAction(new RemoveUserDescription().setDeviceName(devName));
//                }
//
//                // Create an action to delete the machine.
//                // This will do The Right Thing in terms of cordoning/uncordoning and dealing with devices.
//                // This might over-write an existing RemoveFromMachine action -- that is okay, we prefer this one.
//                if (machine != null) {
//                    var action = new DeleteMachine().setMachineName(machineName);
//                    action.setNodeName(nodeName);
//                    machActions.put(machineName, action);
//                }
//            } else {
//                // If the device is attached to a machine, we need to remove it therefrom.
//                // If there is no action yet for the machine, create one.
//                // If there *is* an action, and it is not RemoveFromMachine, leave it be...
//                //  it is a DeleteMachine action which we prefer anyway.
//                if (machine != null) {
//                    var action = machActions.get(machineName);
//                    if (action == null) {
//                        var comp = _liqidInventory.getComputeDeviceStatusForMachine(machine.getMachineId());
//                        var nodeName = _liqidInventory.getK8sNodeNameFromComputeDevice(comp);
//                        action = new RemoveFromMachine().setMachineName(machineName).setNodeName(nodeName);
//                        machActions.put(machineName, action);
//                    }
//
//                    if (action instanceof RemoveFromMachine remAction) {
//                        remAction.addDeviceName(devName);
//                    }
//                }
//            }
//
//            // Also need to remove the device from its group - if it has one
//            if (group != null) {
//                if (!groupActions.containsKey(groupName)) {
//                    var action = new RemoveFromGroup().setGroupName(groupName);
//                    groupActions.put(groupName, action);
//                }
//
//                groupActions.get(groupName).addDeviceName(devName);
//            }
//        }
//
//        machActions.values().forEach(plan::addAction);
//        groupActions.values().forEach(plan::addAction);
//
//        _logger.trace("Exiting %s with %s", fn, plan);
//        return plan;
//    }

    @Override
    public Plan process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             InternalErrorException,
             K8SException,
             LiqidException,
             ProcessingException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

//        initK8sClient();
//
//        // If there is no linkage, tell the user and stop
//        if (!hasLinkage()) {
//            throw new ConfigurationException("No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
//        }
//
//        getLiqidLinkage();
//        initLiqidClient();
//        loadLiqidInventory();
//        var plan = createPlan();
        var plan = new Plan();

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
