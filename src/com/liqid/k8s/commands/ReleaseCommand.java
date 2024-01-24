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
import com.liqid.k8s.layout.GeneralType;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.Action;
import com.liqid.k8s.plan.actions.DeleteMachineAction;
import com.liqid.k8s.plan.actions.RemoveAnnotationsAction;
import com.liqid.k8s.plan.actions.RemoveFromGroupAction;
import com.liqid.k8s.plan.actions.RemoveFromMachineAction;
import com.liqid.k8s.plan.actions.RemoveUserDescriptionAction;
import com.liqid.sdk.LiqidException;
import com.liqid.sdk.Machine;

import java.util.Collection;
import java.util.HashMap;

/**
 * For releasing resources which have been (or will be) removed from the Liqid Cluster
 */
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

    private Plan createPlan() {
        var fn = "createPlan";
        _logger.trace("Entering %s", fn);

        var plan = new Plan();
        var errors = false;
        var errPrefix = getErrorPrefix();

        if (_resourceSpecs != null) {
            var groupActions = new HashMap<String, RemoveFromGroupAction>();
            var machActions = new HashMap<String, Action>();
            for (var devName : _resourceSpecs) {
                var devItem = _liqidInventory.getDeviceItem(devName);
                if (devItem == null) {
                    System.err.printf("%s:Resource '%s' does not exist in the Liqid Cluster\n", errPrefix, devName);
                    errors = true;
                    continue;
                }

                if (!devItem.isAssignedToGroup()) {
                    System.out.printf("NOTE:Resource '%s' is not attached to any group or machine\n", devName);
                    continue;
                }

                var genType = devItem.getGeneralType();
                var group = _liqidInventory.getGroup(devItem.getGroupId());

                Machine machine = null;
                if (devItem.isAssignedToMachine()) {
                    machine = _liqidInventory.getMachine(devItem.getMachineId());
                }

                if (genType == GeneralType.CPU) {
                    // Grab user description from the compute node, use that to generate an action
                    // to remove the annotation from the corresponding worker node.
                    // Also clear out the user description.
                    var nodeName = devItem.getDeviceInfo().getUserDescription();
                    if (nodeName != null) {
                        plan.addAction(new RemoveAnnotationsAction().addNodeName(nodeName));
                        plan.addAction(new RemoveUserDescriptionAction().setDeviceName(devName));
                    }

                    // Create an action to delete the machine.
                    // This will do The Right Thing in terms of cordoning/uncordoning and dealing with devices.
                    // This might over-write an existing RemoveFromMachine action -- that is okay, we prefer this one.
                    if (machine != null) {
                        var machineName = machine.getMachineName();
                        var action = new DeleteMachineAction().setMachineName(machineName);
                        action.setNodeName(nodeName);
                        machActions.put(machineName, action);
                    }
                } else {
                    // If the device is attached to a machine, we need to remove it therefrom.
                    // If there is no action yet for the machine, create one.
                    // If there *is* an action, and it is not RemoveFromMachine, leave it be...
                    //  it is a DeleteMachine action which we prefer anyway.
                    if (machine != null) {
                        var machineName = machine.getMachineName();
                        var action = machActions.get(machineName);
                        if (action == null) {
                            var compDevItem = _liqidInventory.getDeviceItem(machine.getComputeName());
                            var nodeName = compDevItem.getDeviceInfo().getUserDescription();
                            if (nodeName != null) {
                                action = new RemoveFromMachineAction().setMachineName(machineName).setNodeName(nodeName);
                                machActions.put(machineName, action);
                            }
                        }

                        if (action instanceof RemoveFromMachineAction remAction) {
                            remAction.addDeviceName(devName);
                        }
                    }
                }

                // Also need to remove the device from its group - if it has one
                if (group != null) {
                    var groupName = group.getGroupName();
                    if (!groupActions.containsKey(groupName)) {
                        var action = new RemoveFromGroupAction().setGroupName(groupName);
                        groupActions.put(groupName, action);
                    }

                    groupActions.get(groupName).addDeviceName(devName);
                }
            }

            machActions.values().forEach(plan::addAction);
            groupActions.values().forEach(plan::addAction);
        }

        var result = (errors & !_force) ? null : plan;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

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

        initK8sClient();

        // If there is no linkage, tell the user and stop
        if (!hasLinkage()) {
            throw new ConfigurationException("No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
        }

        getLiqidLinkage();
        initLiqidClient();

        var plan = createPlan();

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
