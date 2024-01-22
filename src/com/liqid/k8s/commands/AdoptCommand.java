/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.*;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.LiqidException;

import java.util.Collection;

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

//    /**
//     * Checks the current Liqid and Kubernetes configuration to see if there is anything which would prevent
//     * us from executing the requested command. Some problems can be warnings if _force is set.
//     * In any case where processing cannot continue, we will throw an exception.
//     */
//    private boolean checkConfiguration(
//        final Map<DeviceItem, Node> computeDevices,
//        final Collection<DeviceItem> resourceDevices
//    ) {
//        var fn = "checkConfiguration";
//        _logger.trace("Entering %s with computeDevices=%s, resourceDevices=%s",
//                      fn, computeDevices, resourceDevices);
//
//        var errors = false;
//        var errPrefix = getErrorPrefix();
//
//        if (!super.checkConfiguration(computeDevices)) {
//            errors = true;
//        }
//
//        // Are any of the called-out resources assigned to groups?
//        var allDevs = new LinkedList<>(computeDevices.keySet());
//        allDevs.addAll(resourceDevices);
//        for (var devItem : allDevs) {
//            if (devItem.isAssignedToGroup()) {
//                System.err.printf("%s:Device %s is currently assigned to a group or machine\n",
//                                  errPrefix, devItem.getStatus().getName());
//                errors = true;
//            }
//        }
//
//        var result = !errors;
//        _logger.trace("Exiting %s with %s", fn, result);
//        return result;
//    }
//
//    private Plan createPlan(
//        final Map<DeviceItem, Node> computeDevices,
//        final Collection<DeviceItem> resourceDevices
//    ) {
//        var fn = "createPlan";
//        _logger.trace("Entering %s with computeDevices=%s, resourceDevices=%s",
//                      fn, computeDevices, resourceDevices);
//
//        var plan = new Plan();
//
//        // Create consolidated list of all devices
//        var allDevStats = new LinkedList<>(computeDevices.keySet());
//        allDevStats.addAll(resourceDevices);
//
//        // Any called-out resources assigned to machines or groups? If so, release them.
//        releaseDevicesFromMachines(_liqidInventory, allDevStats, plan);
//        releaseDevicesFromGroups(_liqidInventory, allDevStats, plan);
//
//        // Move all called-out resources to the newly-created group.
//        if (!allDevStats.isEmpty()) {
//            var names = getDeviceNames(allDevStats);
//            plan.addAction(new AssignToGroup().setGroupName(_liqidGroupName).setDeviceNames(names));
//        }
//
//        // Create machines for all the called-out compute resources and move the compute resources into those machines.
//        // Set the device descriptions to refer to the k8s node names while we're here.
//        createMachines(plan, computeDevices);
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

        //TODO

//        initK8sClient();
//
//        // If there is no linkage, tell the user and stop
//        if (!hasLinkage()) {
//            throw new ConfigurationException("No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
//        }
//
//        initLiqidClient();
//
//        var errors = false;
//        var computeResources = new HashMap<DeviceItem, Node>();
//        if ((_processorSpecs != null) && !developComputeList(_liqidInventory, _processorSpecs, computeResources)) {
//            errors = true;
//        }
//
//        var otherResources = new HashSet<DeviceItem>();
//        if ((_resourceSpecs != null) && !developDeviceList(_liqidInventory, _resourceSpecs, otherResources)) {
//            errors = true;
//        }
//
//        if (!checkConfiguration(computeResources, otherResources)) {
//            errors = true;
//        }
//
//        if (errors && !_force) {
//            throw new ConfigurationException("Various configuration problems exist - processing will not continue.");
//        }

        var plan = new Plan();
//        var plan = createPlan(computeResources, otherResources);
        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
