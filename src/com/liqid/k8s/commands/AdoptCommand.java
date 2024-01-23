/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.k8sclient.Node;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.*;
import com.liqid.k8s.layout.DeviceItem;
import com.liqid.k8s.layout.LiqidInventory;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.AssignToGroupAction;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

/**
 * This command is used when new resources (compute or otherwise) have been added to the Liqid Cluster,
 * and should now be accounted for in our k8s configuration.
 * This is just enough like initialize that one might be tempted to apply that logic here,
 * with disastrous results. So don't do that.
 */
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
    private boolean checkConfiguration(
        final Map<DeviceItem, Node> computeDevices,
        final Collection<DeviceItem> resourceDevices
    ) {
        var fn = "checkConfiguration";
        _logger.trace("Entering %s with computeDevices=%s, resourceDevices=%s",
                      fn, computeDevices, resourceDevices);

        var errors = false;
        var errPrefix = getErrorPrefix();

        if (!checkForContradictions(computeDevices, resourceDevices)) {
            errors = true;
        }

        // Are any of the called-out resources assigned to groups?
        var allDevs = new LinkedList<>(computeDevices.keySet());
        allDevs.addAll(resourceDevices);
        for (var devItem : allDevs) {
            if (devItem.isAssignedToGroup()) {
                System.err.printf("%s:Device %s is currently assigned to a group or machine\n",
                                  errPrefix, devItem.getDeviceStatus().getName());
                errors = true;
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    private Plan createPlan(
        final Map<DeviceItem, Node> computeDevices,
        final Collection<DeviceItem> resourceDevices
    ) {
        var fn = "createPlan";
        _logger.trace("Entering %s with computeDevices=%s, resourceDevices=%s",
                      fn, computeDevices, resourceDevices);

        var plan = new Plan();

        // Create consolidated list of all called-out devices
        var allDevItems = new LinkedList<>(computeDevices.keySet());
        allDevItems.addAll(resourceDevices);

        // Any called-out resources assigned to machines or groups? If so, release them.
        releaseDevicesFromMachines(allDevItems, plan);
        releaseDevicesFromGroups(allDevItems, plan);

        // Move all called-out resources to the newly-created group.
        if (!allDevItems.isEmpty()) {
            var names = LiqidInventory.getDeviceNamesFromItems(allDevItems);
            plan.addAction(new AssignToGroupAction().setGroupName(_liqidGroupName).setDeviceNames(names));
        }

        // Create machines for all the called-out compute resources and move the compute resources into those machines.
        // Set the device descriptions to refer to the k8s node names while we're here.
        createMachines(computeDevices, plan);

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

        getLiqidLinkage();
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
