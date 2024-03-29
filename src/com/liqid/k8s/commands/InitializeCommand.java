/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.*;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.*;
import com.liqid.k8s.layout.ClusterLayout;
import com.liqid.k8s.layout.DeviceItem;
import com.liqid.k8s.layout.GeneralType;
import com.liqid.k8s.layout.LiqidInventory;
import com.liqid.k8s.layout.VarianceSet;
import com.liqid.k8s.plan.*;
import com.liqid.k8s.plan.actions.AssignToGroupAction;
import com.liqid.k8s.plan.actions.CreateGroupAction;
import com.liqid.k8s.plan.actions.CreateLinkageAction;
import com.liqid.k8s.plan.actions.DeleteGroupAction;
import com.liqid.k8s.plan.actions.EnableP2PForMachineAction;
import com.liqid.k8s.plan.actions.RemoveAllAnnotationsAction;
import com.liqid.k8s.plan.actions.RemoveLinkageAction;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidException;
import com.liqid.sdk.Machine;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class InitializeCommand extends Command {

    private boolean _allocate;
    private boolean _enableP2P = false;
    private boolean _hasAnnotations = false;
    private boolean _hasGroup = false;
    private boolean _hasLinkage = false;
    private Collection<String> _processorSpecs;
    private Collection<String> _resourceSpecs;

    public InitializeCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public InitializeCommand setAllocate(final Boolean value) { _allocate = value; return this; }
    public InitializeCommand setEnableP2P(final Boolean value) { _enableP2P = value; return this; }
    public InitializeCommand setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    public InitializeCommand setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    public InitializeCommand setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    public InitializeCommand setLiqidUsername(final String value) { _liqidUsername = value; return this; }
    public InitializeCommand setProcessorSpecs(final Collection<String> list) { _processorSpecs = list; return this; }
    public InitializeCommand setProxyURL(final String value) { _proxyURL = value; return this; }
    public InitializeCommand setResourceSpecs(final Collection<String> list) { _resourceSpecs = list; return this; }

    /**
     * Checks the current Liqid and Kubernetes configuration to see if there is anything which would prevent
     * us from executing the requested command. Some problems can be warnings if _force is set.
     * In any case where processing cannot continue, we will throw an exception.
     */
    private boolean checkConfiguration(
        final Map<DeviceItem, Node> computeDevices,
        final Collection<DeviceItem> resourceDevices
    ) throws K8SHTTPError,
             K8SJSONError,
             K8SRequestError {
        var fn = "checkConfiguration";
        _logger.trace("Entering %s with computeDevices=%s, resourceDevices=%s",
                      fn, computeDevices, resourceDevices);

        var errors = false;
        var errPrefix = getErrorPrefix();

        _hasLinkage = hasLinkage();
        _hasAnnotations = hasAnnotations(computeDevices.values());
        if (_hasLinkage) {
            System.err.printf("%s:Linkage already exists between the Kubernetes Cluster and the Liqid Cluster\n", errPrefix);
            errors = true;
        }
        if (_hasAnnotations) {
            System.err.printf("%s:One or more nodes in the Kubernetes Cluster has Liqid annotations\n", errPrefix);
            errors = true;
        }

        if (!checkForContradictions(computeDevices, resourceDevices)) {
            errors = true;
        }

        // Are there any resources assigned to groups?
        var allDevItems = new HashSet<>(computeDevices.keySet());
        allDevItems.addAll(resourceDevices);
        for (var devItem : allDevItems) {
            if (devItem.isAssignedToGroup() || devItem.isAssignedToMachine()) {
                System.err.printf("%s:Device %s is currently assigned to a group or machine\n",
                                  errPrefix,
                                  devItem.getDeviceName());
                errors = true;
            }
        }

        // Does the called-out group already exist?
        if (_liqidInventory.getGroup(_liqidGroupName) != null) {
            _hasGroup = true;
            System.err.printf("%s:Group %s already exists.\n", errPrefix, _liqidGroupName);
            errors = true;
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    private Plan createPlan(
        final Map<DeviceItem, Node> computeDevices,
        final Collection<DeviceItem> resourceDevices
    ) throws InternalErrorException {
        var fn = "createPlan";
        _logger.trace("Entering %s with computeDevices=%s, resourceDevices=%s",
                      fn, computeDevices, resourceDevices);

        var plan = new Plan();
        if (_hasLinkage) {
            plan.addAction(new RemoveLinkageAction());
        }
        if (_hasAnnotations) {
            plan.addAction(new RemoveAllAnnotationsAction());
        }

        Group deletingGroup = null;
        if (_hasGroup) {
            deletingGroup = _liqidInventory.getGroup(_liqidGroupName);
            plan.addAction(new DeleteGroupAction().setGroupName(_liqidGroupName));
        }

        plan.addAction(new CreateLinkageAction().setLiqidAddress(_liqidAddress)
                                                .setEnableP2P(_enableP2P)
                                                .setLiqidGroupName(_liqidGroupName)
                                                .setLiqidUsername(_liqidUsername)
                                                .setLiqidPassword(_liqidPassword));

        plan.addAction(new CreateGroupAction().setGroupName(_liqidGroupName));

        // Create consolidated list of all devices
        var allDevItems = new LinkedList<>(computeDevices.keySet());
        allDevItems.addAll(resourceDevices);

        // Any called-out resources assigned to machines or groups?
        // I mean, other than the configured group? If so, release them.
        var subsetDevItems = new LinkedList<>(allDevItems);
        if (deletingGroup != null) {
            LiqidInventory.removeDeviceItemsInGroup(subsetDevItems, deletingGroup.getGroupId());
        }
        releaseDevicesFromMachines(subsetDevItems, plan);
        releaseDevicesFromGroups(subsetDevItems, plan);

        // Move all called-out resources to the newly-created group.
        var names = LiqidInventory.getDeviceNamesFromItems(allDevItems);
        if (!names.isEmpty()) {
            plan.addAction(new AssignToGroupAction().setGroupName(_liqidGroupName).setDeviceNames(names));
        }

        // Create machines for all the called-out compute resources and move the compute resources into those machines.
        // Set the device descriptions to refer to the k8s node names while we're here.
        // Note that the in-memory DeviceInfo objects' userDescription fields are updated to contain the corresponding
        // node names, and the in-memory Node objects' annotations are updated to refer to the nodes corresponding
        // Liqid Cluster machine names.
        createMachines(computeDevices, plan);

        // Allocate, if requested
        if (_allocate) {
            ClusterLayout layout = createEvenlyAllocatedClusterLayout(computeDevices, resourceDevices);
            if (!createAnnotationsFromClusterLayout(computeDevices.values(), layout, plan)) {
                _logger.trace("Exiting %s with null", fn);
                return null;
            }

            //  Build a proposed LiqidInventory based on the given devices, group, etc.
            var proposedGroup = new Group().setGroupName(_liqidGroupName).setGroupId(1);
            var proposedInventory = new LiqidInventory();
            proposedInventory.notifyGroupCreated(new Group().setGroupName(_liqidGroupName).setGroupId(1));
            var machId = 1;
            for (var compEntry : computeDevices.entrySet()) {
                var devItem = compEntry.getKey();
                var node = compEntry.getValue();
                var machineName = createMachineName(devItem.getDeviceStatus(), node);

                proposedInventory.notifyDeviceCreated(devItem.getDeviceStatus(), devItem.getDeviceInfo());
                proposedInventory.notifyDeviceAssignedToGroup(devItem.getDeviceId(), proposedGroup.getGroupId());

                var machine = new Machine().setMachineName(machineName).setMachineId(machId++);
                proposedInventory.notifyMachineCreated(machine);
                proposedInventory.notifyDeviceAssignedToMachine(devItem.getDeviceId(), machine.getMachineId());
            }
            for (var devItem : resourceDevices) {
                proposedInventory.notifyDeviceCreated(devItem.getDeviceStatus(), devItem.getDeviceInfo());
                proposedInventory.notifyDeviceAssignedToGroup(devItem.getDeviceId(), proposedGroup.getGroupId());
            }

            var allocators = createAllocators(proposedInventory, layout);
            var allocations = createAllocations(allocators);
            if (allocations == null) {
                _logger.trace("Exiting %s with null", fn);
                return null;
            }

            // Create list of machines for which we want to enable P2P
            var p2pMachines = new LinkedList<String>();
            if (_enableP2P) {
                for (var alloc : allocations) {
                    var devIds = alloc.getDeviceIdentifiers();
                    var gpuCount = (int) devIds.stream()
                                               .filter(devId -> _liqidInventory.getDeviceItem(devId).getGeneralType() == GeneralType.GPU)
                                               .count();
                    if (gpuCount > 1) {
                        p2pMachines.add(alloc.getMachineName());
                    }
                }
            }

            var varSet = VarianceSet.createVarianceSet(proposedInventory, allocations);
            var deviceIds = LiqidInventory.getDeviceIdsFromItems(resourceDevices);
            processVarianceSet(deviceIds, varSet, plan);
            for (var machName : p2pMachines) {
                plan.addAction(new EnableP2PForMachineAction().setMachineName(machName));
            }
        }

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
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

        var errors = false;
        var computeResources = new HashMap<DeviceItem, Node>();
        if (_processorSpecs != null) {
            if (!developComputeListFromSpecifications(_processorSpecs, computeResources)) {
                errors = true;
            }
        }

        var otherResources = new HashSet<DeviceItem>();
        if (_resourceSpecs != null) {
            if (!developDeviceListFromSpecifications(_resourceSpecs, otherResources)) {
                errors = true;
            }
        }

        if (!checkConfiguration(computeResources, otherResources)) {
            errors = true;
        }

        Plan plan = null;
        if (!errors || _force) {
            plan = createPlan(computeResources, otherResources);
        }

        if (plan == null) {
            throw new ConfigurationException("Various configuration problems exist - processing will not continue.");
        }

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
