/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.config;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.k8sclient.Node;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.Constants;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.AnnotateNode;
import com.liqid.k8s.plan.actions.AssignToGroup;
import com.liqid.k8s.plan.actions.AssignToMachine;
import com.liqid.k8s.plan.actions.CreateGroup;
import com.liqid.k8s.plan.actions.CreateLinkage;
import com.liqid.k8s.plan.actions.CreateMachine;
import com.liqid.k8s.plan.actions.DeleteGroup;
import com.liqid.k8s.plan.actions.DeleteMachine;
import com.liqid.k8s.plan.actions.RemoveAllAnnotations;
import com.liqid.k8s.plan.actions.RemoveFromGroup;
import com.liqid.k8s.plan.actions.RemoveFromMachine;
import com.liqid.k8s.plan.actions.RemoveLinkage;
import com.liqid.k8s.plan.actions.SetUserDescription;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

class InitializeCommand extends Command {

    private boolean _hasAnnotations;
    private boolean _hasGroup = false;
    private boolean _hasLinkage;
    private Collection<String> _processorSpecs;
    private Collection<String> _resourceSpecs;

    InitializeCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    InitializeCommand setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    InitializeCommand setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    InitializeCommand setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    InitializeCommand setLiqidUsername(final String value) { _liqidUsername = value; return this; }
    InitializeCommand setProcessorSpecs(final Collection<String> list) { _processorSpecs = list; return this; }
    InitializeCommand setResourceSpecs(final Collection<String> list) { _resourceSpecs = list; return this; }

    /**
     * Checks the current Liqid and Kubernetes configuration to see if there is anything which would prevent
     * us from executing the requested command. Some problems can be warnings if _force is set.
     * In any case where processing cannot continue, we will throw an exception.
     */
    private void checkConfiguration(
        final Map<DeviceStatus, Node> computeDevices,
        final Collection<DeviceStatus> resourceDevices
    ) throws ConfigurationException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError {
        var fn = "checkConfiguration";
        _logger.trace("Entering %s with computeDevices=%s, resourceDevices=%s",
                      fn, computeDevices, resourceDevices);

        // Are there any compute device descriptions which contradict the node names?
        var errors = false;
        var errPrefix = getErrorPrefix();
        for (var entry : computeDevices.entrySet()) {
            var ds = entry.getKey();
            var node = entry.getValue();
            var di = _liqidInventory._deviceInfoById.get(entry.getKey().getDeviceId());
            var desc = di.getUserDescription();
            if (!(desc.equals("n/a") || desc.equals(node.getName()))) {
                System.err.printf("%s:Description for resource %s conflicts with node name %s",
                                  errPrefix,
                                  ds.getName(),
                                  node.getName());
                errors = true;
            }
        }

        // Check for existing linkage and annotations
        _hasLinkage = hasLinkage();
        _hasAnnotations = hasAnnotations();

        if (_hasLinkage) {
            System.err.printf("%s:Linkage already exists between the Kubernetes Cluster and the Liqid Cluster", errPrefix);
            errors = true;
        }

        if (_hasAnnotations) {
            System.err.printf("%s:One or more nodes in the Kubernetes Cluster has Liqid annotations", errPrefix);
            errors = true;
        }

        // Are there any resources assigned to groups?
        var allDevs = new LinkedList<>(computeDevices.keySet());
        allDevs.addAll(resourceDevices);
        for (var ds : allDevs) {
            var dr = _liqidInventory._deviceRelationsByDeviceId.get(ds.getDeviceId());
            if (dr._groupId != null) {
                System.err.printf("%s:Device %s is currently assigned to a group or machine\n", errPrefix, ds.getName());
                errors = true;
            }
        }

        // Does the called-out group already exist?
        if (_liqidInventory._groupsByName.containsKey(_liqidGroupName)) {
            _hasGroup = true;
            System.err.printf("%s:Group %s already exists.\n", errPrefix, _liqidGroupName);
            errors = true;
        }

        if (errors && !_force) {
            var ex = new ConfigurationException("Various configuration problems exist - processing will not continue.");
            _logger.throwing(ex);
            throw ex;
        }

        _logger.trace("Exiting %s", fn);
    }

    private Plan createPlan(
        final Map<DeviceStatus, Node> computeDevices,
        final Collection<DeviceStatus> resourceDevices
    ) {
        var fn = "createPlan";
        _logger.trace("Entering %s with computeDevices=%s, resourceDevices=%s",
                      fn, computeDevices, resourceDevices);

        var plan = new Plan();
        if (_hasLinkage) {
            plan.addAction(new RemoveLinkage());
        }
        if (_hasAnnotations) {
            plan.addAction(new RemoveAllAnnotations());
        }
        if (_hasGroup) {
            plan.addAction(new DeleteGroup().setGroupName(_liqidGroupName));
        }

        plan.addAction(new CreateLinkage().setLiqidAddress(_liqidAddress)
                                          .setLiqidGroupName(_liqidGroupName)
                                          .setLiqidUsername(_liqidUsername)
                                          .setLiqidPassword(_liqidPassword));

        plan.addAction(new CreateGroup().setGroupName(_liqidGroupName));

        // Create consolidated list of all devices
        var allDevStats = new LinkedList<>(computeDevices.keySet());
        allDevStats.addAll(resourceDevices);

        // Any called-out resources assigned to machines or groups? If so, release them.
        releaseDevicesFromMachines(plan, allDevStats);
        releaseDevicesFromGroups(plan, allDevStats);

        // Move all called-out resources to the newly-created group.
        var names = allDevStats.stream().map(DeviceStatus::getName).collect(Collectors.toCollection(TreeSet::new));
        plan.addAction(new AssignToGroup().setGroupName(_liqidGroupName).setDeviceNames(names));

        // Create machines for all the called-out compute resources and move the compute resources into those machines.
        // Set the device descriptions to refer to the k8s node names while we're here.
        createMachines(plan, computeDevices);

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }

    private void createMachines(
        final Plan plan,
        final Map<DeviceStatus, Node> computeDevices
    ) {
        // Create machine, add compute device to machine, set compute device description to node name
        // machine name is limited to 22 characters, may not contain spaces, and must start with an alpha character.
        // We assume the compute devices have already been added to the targeted group.

        // We're going to do it in order by pcpu{n} name, just because it is cleaner.
        var orderedMap = new TreeMap<Integer, DeviceStatus>();
        for (var ds : computeDevices.keySet()) {
            Integer key = Integer.parseInt(ds.getName().substring(4));
            orderedMap.put(key, ds);
        }

        for (var entry : orderedMap.entrySet()) {
            var ds = entry.getValue();
            var node = computeDevices.get(ds);
            var devName = ds.getName();
            var nodeName = node.getName();

            var machineName = String.format("%s-%s", devName, nodeName);
            if (machineName.length() > 22) {
                machineName = machineName.substring(0, 22);
            }

            plan.addAction(new CreateMachine().setMachineName(machineName).setGroupName(_liqidGroupName));
            plan.addAction(new AssignToMachine().setMachineName(machineName).addDeviceName(devName));
            plan.addAction(new SetUserDescription().setDeviceName(devName).setDescription(nodeName));
            plan.addAction(new AnnotateNode().setNodeName(nodeName)
                                             .addAnnotation(Constants.K8S_ANNOTATION_MACHINE_NAME, machineName));
        }
    }

    /**
     * Based on processor and resource lists, we populate containers of compute device information and of
     * non-compute resources. If we find anomalies and _force is not set, throw an exception.
     */
    private void getDeviceList(
        final Map<DeviceStatus, Node> computeDevices,
        final Collection<DeviceStatus> resourceDevices
    ) throws ConfigurationException, K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "developDeviceList";
        _logger.trace("Entering %s with computeDevices=%s resourceDevices=%s",
                      fn, computeDevices, resourceDevices);

        var errors = false;
        var errPrefix = getErrorPrefix();

        for (var spec : _processorSpecs) {
            var split = spec.split(":");
            if (split.length != 2) {
                System.err.printf("ERROR:Invalid format for spec '%s'\n", spec);
                errors = true;
            }

            var devName = split[0];
            var nodeName = split[1];

            var devStat = _liqidInventory._deviceStatusByName.get(devName);
            if (devStat == null) {
                System.err.printf("%s:Compute resource '%s' is not in the Liqid Cluster\n", errPrefix, devName);
                errors = true;
            }

            Node node = null;
            try {
                node = _k8sClient.getNode(nodeName);
            } catch (K8SHTTPError ex) {
                if (ex.getResponseCode() == 404) {
                    System.err.printf("%s:Worker node '%s' is not in the Kubernetes Cluster\n", errPrefix, nodeName);
                    errors = true;
                } else {
                    throw ex;
                }
            }

            if ((devStat != null) && (node != null)) {
                computeDevices.put(devStat, node);
            }
        }

        for (var spec : _resourceSpecs) {
            var devStat = _liqidInventory._deviceStatusByName.get(spec);
            if (devStat == null) {
                System.err.printf("%s:Resource '%s' is not in the Liqid Cluster\n", errPrefix, spec);
                errors = true;
            } else {
                resourceDevices.add(devStat);
            }
        }

        if (errors && _force) {
            var ex = new ConfigurationException("Various configuration problems exist - processing will not continue.");
            _logger.throwing(ex);
            throw ex;
        }

        _logger.trace("Exiting %s with computeDevices=%s, resourceDevices=%s",
                      fn, computeDevices, resourceDevices);
    }

    /**
     * Adds actions to the given plan to efficiently release all indicated devices from their containing machines.
     */
    private void releaseDevicesFromMachines(
        final Plan plan,
        final Collection<DeviceStatus> devices
    ) {
        // Iterate over the machines so that we can do multiple devices per machine.
        // In the case where we're removing all the devices for a machine, just delete the machine.

        for (var entry : _liqidInventory._deviceStatusByMachineId.entrySet()) {
            // Find the group to which this machine belongs.
            // If that group's name matches _liqidGroupName, the whole group is going to be deleted,
            // and we don't need to do this mess for this machine.
            var machId = entry.getKey();
            var machine = _liqidInventory._machinesById.get(machId);
            var grpId = machine.getGroupId();
            var group = _liqidInventory._groupsById.get(grpId);
            if (!group.getGroupName().equals(_liqidGroupName)) {
                var machDevs = entry.getValue();
                var devsToRemove = new LinkedList<DeviceStatus>();
                getIntersection(devices, machDevs, devsToRemove);

                if (!devsToRemove.isEmpty()) {
                    if (devsToRemove.size() == machDevs.size()) {
                        plan.addAction(new DeleteMachine().setMachineName(machine.getMachineName()));
                    } else {
                        var devNames = devsToRemove.stream()
                                                   .map(DeviceStatus::getName)
                                                   .collect(Collectors.toCollection(TreeSet::new));
                        plan.addAction(new RemoveFromMachine().setMachineName(machine.getMachineName())
                                                              .setDeviceNames(devNames));
                    }
                }
            }
        }
    }

    /**
     * Adds actions to the given plan to efficiently release all indicated devices from their containing groups.
     */
    private void releaseDevicesFromGroups(
        final Plan plan,
        final Collection<DeviceStatus> devices
    ) {
        // Iterate over the groups so that we can do multiple devices per group.
        // In the case where we're removing all the devices for a group, just delete the group.
        for (var entry : _liqidInventory._deviceStatusByGroupId.entrySet()) {
            // If this group's name matches the liqid group name, we're going to delete it anyway.
            var groupId = entry.getKey();
            var group = _liqidInventory._groupsById.get(groupId);
            if (!group.getGroupName().equals(_liqidGroupName)) {
                var grpDevs = entry.getValue();
                var devsToRemove = new LinkedList<DeviceStatus>();
                getIntersection(devices, grpDevs, devsToRemove);

                if (!devsToRemove.isEmpty()) {
                    if (devsToRemove.size() == grpDevs.size()) {
                        plan.addAction(new DeleteGroup().setGroupName(group.getGroupName()));
                    } else {
                        var names = devsToRemove.stream()
                                                .map(DeviceStatus::getName)
                                                .collect(Collectors.toCollection(TreeSet::new));
                        plan.addAction(new RemoveFromGroup().setGroupName(group.getGroupName()).setDeviceNames(names));
                    }
                }
            }
        }
    }

    @Override
    public Plan process(
    ) throws ConfigurationException,
             InternalErrorException,
             K8SException,
             LiqidException,
             ProcessingException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

        initK8sClient();
        initLiqidClient();
        getLiqidInventory();

        var computeResources = new HashMap<DeviceStatus, Node>();
        var otherResources = new LinkedList<DeviceStatus>();
        getDeviceList(computeResources, otherResources);

        checkConfiguration(computeResources, otherResources);
        var plan = createPlan(computeResources, otherResources);

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
