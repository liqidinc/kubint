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
import com.liqid.k8s.layout.LiqidInventory;
import com.liqid.k8s.layout.VarianceSet;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.EnableP2PForMachineAction;
import com.liqid.sdk.LiqidException;

import java.util.LinkedList;

public class ComposeCommand extends Command {

    public Boolean _p2pOverride = false;

    public ComposeCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public ComposeCommand setEnableP2POverride(final Boolean value) { _p2pOverride = value; return this; }
    public ComposeCommand setProxyURL(final String value) { _proxyURL = value; return this; }

    @Override
    public Plan process(
    ) throws ConfigurationDataException,
             ConfigurationException,
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

        var nodes = _k8sClient.getNodes();
        var desiredLayout = createClusterLayoutFromAnnotations(nodes);
        if (desiredLayout == null) {
            throw new ConfigurationDataException("Various configuration problems exist - processing will not continue.");
        }
        System.out.println("Desired Layout:");
        desiredLayout.show("| ");

        var allocators = createAllocators(_liqidInventory, desiredLayout);
        if (allocators == null) {
            _logger.trace("Exiting %s with null", fn);
            return null;
        }

        var allocations = createAllocations(allocators);
        if (allocations == null) {
            _logger.trace("Exiting %s with null", fn);
            return null;
        }

        // Create list of machines for which we want to enable P2P
        var p2pMachines = new LinkedList<String>();
        if (_liqidEnableP2P || _p2pOverride) {
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

        var varSet = VarianceSet.createVarianceSet(_liqidInventory, allocations);
        var devItems = _liqidInventory.getDeviceItems();
        LiqidInventory.removeDeviceItemsOfType(devItems, GeneralType.CPU);
        LiqidInventory.removeDeviceItemsInAnyMachine(devItems);
        var deviceIds = LiqidInventory.getDeviceIdsFromItems(devItems);

        var plan = new Plan();
        processVarianceSet(deviceIds, varSet, plan);
        for (var machName : p2pMachines) {
            plan.addAction(new EnableP2PForMachineAction().setMachineName(machName));
        }

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
