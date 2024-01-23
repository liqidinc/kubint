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
import com.liqid.k8s.layout.LiqidInventory;
import com.liqid.k8s.layout.VarianceSet;
import com.liqid.k8s.plan.*;
import com.liqid.k8s.plan.actions.AssignToGroupAction;
import com.liqid.k8s.plan.actions.CreateGroupAction;
import com.liqid.k8s.plan.actions.CreateLinkageAction;
import com.liqid.k8s.plan.actions.DeleteGroupAction;
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

import static com.liqid.k8s.Constants.K8S_ANNOTATION_MACHINE_NAME;

public class InitializeCommand extends Command {

    private boolean _allocate;
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
            System.err.printf("%s:Linkage already exists between the Kubernetes Cluster and the Liqid Cluster", errPrefix);
            errors = true;
        }
        if (_hasAnnotations) {
            System.err.printf("%s:One or more nodes in the Kubernetes Cluster has Liqid annotations", errPrefix);
            errors = true;
        }

        // Are there any compute device descriptions which contradict the node names?
        for (var entry : computeDevices.entrySet()) {
            var devItem = entry.getKey();
            var node = entry.getValue();
            var desc = devItem.getDeviceInfo().getUserDescription();
            if ((desc != null) && (!desc.equals("n/a")) && !desc.equals(node.getName())) {
                System.err.printf("%s:User description for device '%s' is not set to the corresponding node name '%s'\n",
                                  errPrefix,
                                  devItem.getDeviceName(),
                                  node.getName());
            }

            var machineAnnoKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
            var machineAnnotation = node.metadata.annotations.get(machineAnnoKey);
            if ((machineAnnotation != null) &&
                (!_liqidInventory.getMachine(machineAnnotation).getComputeName().equals(devItem.getDeviceName()))) {
                System.err.printf("%s:node name '%s' has an incorrect annotation referencing machine name '%s'\n",
                                  errPrefix,
                                  node.getName(),
                                  machineAnnotation);
                errors = true;
            }
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
        if (_hasGroup) {
            plan.addAction(new DeleteGroupAction().setGroupName(_liqidGroupName));
        }

        plan.addAction(new CreateLinkageAction().setLiqidAddress(_liqidAddress)
                                                .setLiqidGroupName(_liqidGroupName)
                                                .setLiqidUsername(_liqidUsername)
                                                .setLiqidPassword(_liqidPassword));

        plan.addAction(new CreateGroupAction().setGroupName(_liqidGroupName));

        // Create consolidated list of all devices
        var allDevItems = new LinkedList<>(computeDevices.keySet());
        allDevItems.addAll(resourceDevices);

        // Any called-out resources assigned to machines or groups? If so, release them.
        releaseDevicesFromMachines(allDevItems, plan);
        releaseDevicesFromGroups(allDevItems, plan);

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
            System.out.println("================" + allocations);//TODO remove
            if (allocations == null) {
                _logger.trace("Exiting %s with null", fn);
                return null;
            }

            var varSet = VarianceSet.createVarianceSet(proposedInventory, allocations);
            System.out.println("================" + varSet);//TODO remove
            var deviceIds = LiqidInventory.getDeviceIdsFromItems(resourceDevices);
            processVarianceSet(deviceIds, varSet, plan);
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

        if (errors && !_force) {
            throw new ConfigurationException("Various configuration problems exist - processing will not continue.");
        }

        var plan = createPlan(computeResources, otherResources);
        if (plan == null) {
            throw new ConfigurationException("Various configuration problems exist - processing will not continue.");
        }

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
