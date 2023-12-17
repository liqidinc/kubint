/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.config;

import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.k8sclient.Node;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.GeneralType;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.sdk.DeviceQueryType;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_MACHINE_NAME;
import static com.liqid.k8s.config.CommandType.INITIALIZE;
import static com.liqid.k8s.plan.LiqidInventory.getLiqidInventory;

class InitializeCommand extends Command {

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

    private void attachDevices(
        final Group group,
        final Collection<DeviceStatus> devices
    ) throws LiqidException {
        var names = devices.stream().map(DeviceStatus::getName).collect(Collectors.toCollection(LinkedList::new));
        var nameStr = String.join(", ", names);
        System.out.printf("Attaching devices %s to group %s...\n", nameStr, group.getGroupName());
        _liqidClient.groupPoolEdit(group.getGroupId());
        for (var ds : devices) {
            _liqidClient.addDeviceToGroup(ds.getDeviceId(), group.getGroupId());
        }
        _liqidClient.groupPoolDone(group.getGroupId());
    }

    private boolean checkConfiguration(
        final Map<DeviceStatus, Node> computeDevices,
        final List<DeviceStatus> resourceDevices
    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "checkConfiguration";
        _logger.trace("Entering %s", fn);

        // Are there any compute device descriptions which contradict the node names?
        var errors = false;
        var errPrefix = getErrorPrefix();
        for (var entry : computeDevices.entrySet()) {
            var ds = entry.getKey();
            var node = entry.getValue();
            var di = _liqidInventory._deviceInfoById.get(entry.getKey().getDeviceId());
            var desc = di.getUserDescription();
            if (!(desc.equals("n/a") || desc.equals(node.getName()))) {
                System.err.printf("%s:Description for resource %s conflicts with node name %s\n",
                                  errPrefix, ds.getName(), node.getName());
                if (!_force) {
                    errors = true;
                }
            }
        }

        // Check for existing linkage and annotations
        if (!checkForExistingLinkage(INITIALIZE.getToken())) {
            errors = true;
        }

        if (!checkForExistingAnnotations(INITIALIZE.getToken())) {
            errors = true;
        }

        // Are there any resources assigned to groups?
        // If so, we cannot continue unless -force is set
        var allDevs = new LinkedList<>(computeDevices.keySet());
        allDevs.addAll(resourceDevices);
        for (var ds : allDevs) {
            var dr = _liqidInventory._deviceRelationsByDeviceId.get(ds.getDeviceId());
            if (dr._groupId != null) {
                System.err.printf("%s:Device %s is currently assigned to a group or machine\n", errPrefix, ds.getName());
                if (!_force) {
                    errors = true;
                }
            }
        }

        // Does the called-out group already exist?
        if (_liqidInventory._groupsByName.containsKey(_liqidGroupName)) {
            System.err.printf("%s:Group %s already exists.\n", errPrefix, _liqidGroupName);
            if (!_force) {
                errors = true;
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    private void createMachines(
        final Group group,
        final Map<DeviceStatus, Node> computeDevices
    ) throws K8SHTTPError, K8SRequestError, LiqidException {
        // Create machine, add compute device to machine, set compute device description to node name
        // machine name is limited to 22 characters, may not contain spaces, and must start with an alpha character.
        // We assume the compute devices have already been added to the targeted group.

        // We're going to do it in order by pcpu{n} name, just because it is cleaner.
        var orderedMap = new TreeMap<Integer, DeviceStatus>();
        for (var ds : computeDevices.keySet()) {
            Integer key = Integer.parseInt(ds.getName().substring(4));
            orderedMap.put(key, ds);
        }

        var groupId = group.getGroupId();
        for (var entry : orderedMap.entrySet()) {
            var ds = entry.getValue();
            var node = computeDevices.get(ds);
            var devId = ds.getDeviceId();
            var devName = ds.getName();
            var nodeName = node.getName();

            var machineName = String.format("%s-%s", devName, nodeName);
            if (machineName.length() > 22) {
                machineName = machineName.substring(0, 22);
            }

            System.out.printf("Creating machine %s...\n", machineName);
            _liqidClient.createMachine(group.getGroupId(), machineName);
            var machine = _liqidClient.getMachineByName(machineName);
            var machId = machine.getMachineId();


            _liqidClient.editFabric(machId);
            _liqidClient.addDeviceToMachine(devId, groupId, machId);
            _liqidClient.reprogramFabric(machId);

            _liqidClient.createDeviceDescription(DeviceQueryType.COMPUTE, devId, nodeName);

            // Annotate the worker node - clear out resource counts just in case something is left over from
            // a previous installation.
            var annos = new HashMap<String, String>();
            var annoKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
            annos.put(annoKey, machineName);
            for (var genType : GeneralType.values()) {
                annoKey = createAnnotationKeyForDeviceType(genType);
                annos.put(annoKey, null);
            }
            System.out.printf("Writing annotations to node %s...\n", nodeName);
            _k8sClient.updateAnnotationsForNode(nodeName, annos);
        }
    }

    private boolean doConfiguration(
        final Map<DeviceStatus, Node> computeDevices,
        final List<DeviceStatus> resourceDevices
    ) throws K8SHTTPError, K8SRequestError, LiqidException {
        var fn = "doConfiguration";
        _logger.trace("Entering %s compDevs=%s resDevs=%s", fn, computeDevices, resourceDevices);

        // Create linkage between Kubernetes and Liqid
        createLinkage();

        // If there is already a group, delete it. This will free up all the resources, machines, etc.
        // Then recreate the empty group.
        // Then reload the inventory - it's messy, but the most reliable way to do it.
        if (_liqidInventory._groupsByName.containsKey(_liqidGroupName)) {
            System.out.printf("Deleting existing group '%s'...\n", _liqidGroupName);
            _liqidClient.deleteGroup(_liqidInventory._groupsByName.get(_liqidGroupName).getGroupId());
        }

        System.out.printf("Creating group '%s'...\n", _liqidGroupName);
        _liqidClient.createGroup(_liqidGroupName);

        _liqidInventory = getLiqidInventory(_liqidClient, _logger);

        // Create consolidated list of all devices
        var allDevStats = new LinkedList<>(computeDevices.keySet());
        allDevStats.addAll(resourceDevices);

        // Any called-out resources assigned to machines or groups? If so, release them.
        // Then move all called-out resources to the newly-created group.
        // We don't need to reload the liqid config again - we don't care where the resources are...
        // or rather, we already know where they are, in spite of where the config containers *think* they are.
        releaseDevices(allDevStats);
        var group = _liqidInventory._groupsByName.get(_liqidGroupName);
        attachDevices(group, allDevStats);

        // Create machines for all the called-out compute resources and move the compute resources into those machines.
        // Set the device descriptions to refer to the k8s node names while we're here.
        createMachines(group, computeDevices);

        _logger.trace("Exiting %s true", fn);
        return true;
    }

    /**
     * Based on processor and resource lists, we populate containers of compute device information and of
     * non-compute resources.
     */
    private boolean getDeviceList(
        final Map<DeviceStatus, Node> computeDevices,
        final List<DeviceStatus> resourceDevices
    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "developDeviceList";
        _logger.trace("Entering %s", fn);

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
                if (!_force) {
                    errors = true;
                }
            }

            Node node = null;
            try {
                node = _k8sClient.getNode(nodeName);
            } catch (K8SHTTPError ex) {
                if (ex.getResponseCode() == 404) {
                    System.err.printf("%s:Worker node '%s' is not in the Kubernetes Cluster\n", errPrefix, nodeName);
                    if (!_force) {
                        errors = true;
                    }
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
                if (!_force) {
                    errors = true;
                }
            } else {
                resourceDevices.add(devStat);
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s, compDevs=%s, resDevs=%s",
                      fn, result, computeDevices, resourceDevices);
        return result;
    }

    private void releaseDevices(
        final Collection<DeviceStatus> devices
    ) throws LiqidException {
        // Release all of the devices in the list which are currently assigned to machines.
        // If the machine goes empty, delete it as a courtesy.
        for (var entry : _liqidInventory._deviceStatusByMachineId.entrySet()) {
            var machId = entry.getKey();
            var machDevs = entry.getValue();
            var devsToRemove = new LinkedList<DeviceStatus>();
            getIntersection(devices, machDevs, devsToRemove);

            var machine = _liqidInventory._machinesById.get(machId);
            if (!devsToRemove.isEmpty()) {
                var names = devsToRemove.stream().map(DeviceStatus::getName).collect(Collectors.toCollection(LinkedList::new));
                var nameStr = String.join(", ", names);

                if (devsToRemove.size() == machDevs.size()) {
                    // the machine will be empty upon removing all called-out devices. Just delete the machine.
                    System.out.printf("Deleting machine %s to remove devices %s...\n", machine.getMachineName(), nameStr);
                    _liqidClient.deleteMachine(machId);
                } else {
                    // there will still be devices in the machine -- need to remove the called-out devices only.
                    System.out.printf("Removing devices %s from machine %s...\n", nameStr, machine.getMachineName());
                    _liqidClient.editFabric(machId);
                    for (var devStat : devsToRemove) {
                        _liqidClient.removeDeviceFromMachine(devStat.getDeviceId(), machine.getGroupId(), machId);
                    }
                    _liqidClient.reprogramFabric(machId);
                }
            }
        }

        for (var entry : _liqidInventory._deviceStatusByGroupId.entrySet()) {
            var groupId = entry.getKey();
            var grpDevs = entry.getValue();
            var devsToRemove = new LinkedList<DeviceStatus>();
            getIntersection(devices, grpDevs, devsToRemove);

            var group = _liqidInventory._groupsById.get(groupId);
            if (!devsToRemove.isEmpty()) {
                var names = devsToRemove.stream().map(DeviceStatus::getName).collect(Collectors.toCollection(LinkedList::new));
                var nameStr = String.join(", ", names);

                if (devsToRemove.size() == grpDevs.size()) {
                    // the group will be empty upon removing all called-out devices - just delete the group.
                    System.out.printf("Deleting group %s to remove devices %s...\n", group.getGroupName(), nameStr);
                    _liqidClient.deleteGroup(groupId);
                } else {
                    // there will still be devices in the group -- need to remove the called-out devices only.
                    System.out.printf("Removing devices %s from group %s...\n", nameStr, group.getGroupName());
                    _liqidClient.groupPoolEdit(groupId);
                    for (var devStat : devsToRemove) {
                        _liqidClient.removeDeviceFromGroup(devStat.getDeviceId(), groupId);
                    }
                    _liqidClient.groupPoolDone(groupId);
                }
            }
        }
    }

    @Override
    public boolean process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError,
             LiqidException {
        var fn = INITIALIZE.getToken() + ":process";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        if (!initLiqidClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        _liqidInventory = getLiqidInventory(_liqidClient, _logger);

        var computeResources = new HashMap<DeviceStatus, Node>();
        var otherResources = new LinkedList<DeviceStatus>();
        if (!getDeviceList(computeResources, otherResources)) {
            System.err.println("Errors prevent further processing");
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        if (!checkConfiguration(computeResources, otherResources) && !_force) {
            System.err.println("Errors prevent further processing");
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        if (!doConfiguration(computeResources, otherResources)) {
            System.err.println("Errors occurred during processing");
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
