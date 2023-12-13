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
    public void process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError,
             LiqidException {
        var fn = "process";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        if (!getLiqidLinkage()) {
            throw new ConfigurationException("No linkage exists between the Kubernetes Cluster and a Liqid Cluster.");
        }

        if (!initLiqidClient()) {
            System.err.println("ERROR:Cannot connect to the Liqid Cluster");
            _logger.trace("Exiting %s", fn);
            return;
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
            _logger.trace("Exiting %s", fn);
        }

        // Create a map of all devices in the cluster, by device name.
        // We do this so that we can look them up from the preDevice object.
        // We do it this way in order to leverage the efficiency of getting all the device status objects at once,
        // rather than looking them up one-by-one per preDevice.
        var devStats = _liqidClient.getAllDevicesStatus();
        var devStatMap = devStats.stream()
                                 .collect(Collectors.toMap(DeviceStatus::getName, dev -> dev, (a, b) -> b, HashMap::new));

        // Now do something similar with managed entities
        var mes = _liqidClient.getManagedEntities();
        var pciMap = new HashMap<String, ManagedEntity>();
        for (var me : mes) {
            var key = me.getPCIVendorId() + ":" + me.getPCIDeviceId();
            pciMap.put(key, me);
        }

        // Grab all DeviceInfo things and store them by name
        var infosList = new LinkedList<DeviceInfo>();
        infosList.addAll(_liqidClient.getComputeDeviceInfo());
        infosList.addAll(_liqidClient.getFPGADeviceInfo());
        infosList.addAll(_liqidClient.getGPUDeviceInfo());
        infosList.addAll(_liqidClient.getMemoryDeviceInfo());
        infosList.addAll(_liqidClient.getNetworkDeviceInfo());
        infosList.addAll(_liqidClient.getStorageDeviceInfo());
        var infos = new HashMap<String, DeviceInfo>();
        for (var info : infosList) {
            infos.put(info.getName(), info);
        }

        // One more map - this one stores machines by machine ID
        var machines = _liqidClient.getMachines();
        var machineMap = machines.stream()
                                 .collect(Collectors.toMap(Machine::getMachineId, mach -> mach, (a, b) -> b, HashMap::new));

        System.out.println("Resource within the group:");
        var preDevices = _liqidClient.getDevices(null, groupId, null);
        System.out.println("  ---TYPE---  --NAME--  ----ID----  --VENDOR--  --MODEL---  -------MACHINE--------  --DESCRIPTION--");
        for (var preDev : preDevices) {
            var ds = devStatMap.get(preDev.getDeviceName());
            var pciKey = ds.getPCIVendorId() + ":" + ds.getPCIDeviceId();
            var me = pciMap.get(pciKey);
            var info = infos.get(preDev.getDeviceName());

            var machStr = "<none>";
            var machId = preDev.getMachineId();
            if (machId != 0) {
                var mach = machineMap.get(machId);
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
        for (var mach : machines) {
            if (mach.getGroupId().equals(groupId)) {
                System.out.printf("  %-22s  0x%08x  %s\n", mach.getMachineName(), mach.getMachineId(), mach.getComputeName());
            }
        }

        // All done
        logoutFromLiqidCluster();
        _logger.trace("Exiting %s", fn);
    }
}
