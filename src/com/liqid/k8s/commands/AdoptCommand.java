/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.k8sclient.Node;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.*;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.AssignToGroup;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class AdoptCommand extends Command {

    private Collection<String> _processorSpecs;
    private Collection<String> _resourceSpecs;

    public AdoptCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public AdoptCommand setProcessorSpecs(final Collection<String> list) {_processorSpecs = list; return this; }
    public AdoptCommand setProxyURL(final String value) {_proxyURL = value; return this; }
    public AdoptCommand setResourceSpecs(final Collection<String> list) {_resourceSpecs = list; return this; }

    /**
     * Checks the current Liqid and Kubernetes configuration to see if there is anything which would prevent
     * us from executing the requested command. Some problems can be warnings if _force is set.
     * In any case where processing cannot continue, we will throw an exception.
     */
    protected boolean checkConfiguration(
        final Map<DeviceStatus, Node> computeDevices,
        final Collection<DeviceStatus> resourceDevices
    ) {
        var fn = "checkConfiguration";
        _logger.trace("Entering %s with computeDevices=%s, resourceDevices=%s",
                      fn, computeDevices, resourceDevices);

        var errors = false;
        var errPrefix = getErrorPrefix();

        if (!super.checkConfiguration(computeDevices)) {
            errors = true;
        }

        // Are any of the called-out resources assigned to groups?
        var allDevs = new LinkedList<>(computeDevices.keySet());
        allDevs.addAll(resourceDevices);
        for (var ds : allDevs) {
            var dr = _liqidInventory._deviceRelationsByDeviceId.get(ds.getDeviceId());
            if (dr._groupId != null) {
                System.err.printf("%s:Device %s is currently assigned to a group or machine\n", errPrefix, ds.getName());
                errors = true;
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    private Plan createPlan(
        final Map<DeviceStatus, Node> computeDevices,
        final Collection<DeviceStatus> resourceDevices
    ) {
        var fn = "createPlan";
        _logger.trace("Entering %s with computeDevices=%s, resourceDevices=%s",
                      fn, computeDevices, resourceDevices);

        var plan = new Plan();

        // Create consolidated list of all devices
        var allDevStats = new LinkedList<>(computeDevices.keySet());
        allDevStats.addAll(resourceDevices);

        // Any called-out resources assigned to machines or groups? If so, release them.
        releaseDevicesFromMachines(plan, allDevStats);
        releaseDevicesFromGroups(plan, allDevStats);

        // Move all called-out resources to the newly-created group.
        if (!allDevStats.isEmpty()) {
            var names = allDevStats.stream().map(DeviceStatus::getName).collect(Collectors.toCollection(TreeSet::new));
            plan.addAction(new AssignToGroup().setGroupName(_liqidGroupName).setDeviceNames(names));
        }

        // Create machines for all the called-out compute resources and move the compute resources into those machines.
        // Set the device descriptions to refer to the k8s node names while we're here.
        createMachines(plan, computeDevices);

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
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

        initLiqidClient();
        getLiqidInventory();

        var errors = false;
        var computeResources = new HashMap<DeviceStatus, Node>();
        if ((_processorSpecs != null) && !developComputeList(_processorSpecs, computeResources)) {
            errors = true;
        }

        var otherResources = new LinkedList<DeviceStatus>();
        if ((_resourceSpecs != null) && !developDeviceList(_resourceSpecs, otherResources)) {
            errors = true;
        }

        if (!checkConfiguration(computeResources, otherResources)) {
            errors = true;
        }

        if (errors && !_force) {
            throw new ConfigurationException("Various configuration problems exist - processing will not continue.");
        }

        var plan = createPlan(computeResources, otherResources);
        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
