/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidException;
import com.liqid.sdk.Machine;
import com.liqid.sdk.ManagedEntity;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.stream.Collectors;

import static com.liqid.k8s.annotate.CommandType.RESOURCES;

class ResourcesCommand extends Command {

    ResourcesCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    @Override
    public boolean process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError,
             LiqidException {
        var fn = RESOURCES.getToken() + ":process";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        if (!getLiqidLinkage()) {
            throw new ConfigurationException("No linkage exists between the Kubernetes Cluster and a Liqid Cluster.");
        }

        if (!initLiqidClient()) {
            System.err.println("ERROR:Cannot connect to the Liqid Cluster");
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        Integer groupId = null;
        Group group = null;
        try {
            groupId = _liqidClient.getGroupIdByName(_liqidGroupName);
            group = _liqidClient.getGroup(groupId);
            System.out.printf("Found Liqid Cluster group ID=%d Name=%s\n", group.getGroupId(), group.getGroupName());
        } catch (LiqidException ex) {
            // If we end up here, it's probably because the group does not exist
            System.err.println("ERROR:Liqid group '" + _liqidGroupName + "' does not exist");
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        getLiqidInventory();

        System.out.println("Resource within the group:");
        var preDevices = _liqidClient.getDevices(null, groupId, null);
        System.out.println("  ---TYPE---  --NAME--  ----ID----  --VENDOR--  --MODEL---  -------MACHINE--------  --DESCRIPTION--");
        for (var preDev : preDevices) {
            var ds = _deviceStatusByName.get(preDev.getDeviceName());
            var pciKey = ds.getPCIVendorId() + ":" + ds.getPCIDeviceId();
            var me = _managedEntitiesByCompositeIds.get(pciKey);
            var info = _deviceInfoByName.get(preDev.getDeviceName());

            var machStr = "<none>";
            var machId = preDev.getMachineId();
            if (machId != 0) {
                var mach = _machinesById.get(machId);
                machStr = mach.getMachineName();
            }

            System.out.printf("  %-10s  %-8s  0x%08x  %-10s  %-10s  %-22s  %s\n",
                              ds.getDeviceType(),
                              ds.getName(),
                              ds.getDeviceId(),
                              me.getManufacturer(),
                              me.getModel(),
                              machStr,
                              info.getUserDescription());
        }

        System.out.println("Machines within the group:");
        System.out.println("  ----------------------  ----ID----  --CPU--");
        for (var mach : _machinesById.values()) {
            if (mach.getGroupId().equals(groupId)) {
                System.out.printf("  %-22s  0x%08x  %s\n", mach.getMachineName(), mach.getMachineId(), mach.getComputeName());
            }
        }

        // All done
        logoutFromLiqidCluster();
        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
