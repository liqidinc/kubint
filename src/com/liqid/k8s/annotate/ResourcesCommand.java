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
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidException;

import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.liqid.k8s.annotate.CommandType.RESOURCES;

class ResourcesCommand extends Command {

    private boolean _allFlag = false;

    ResourcesCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    public ResourcesCommand setAll(final Boolean flag) { _allFlag = flag; return this; }

    /**
     * Displays devices
     * @param group Reference to Group if we want to limit the display to resources in that group, else null
     * @throws LiqidException If we have trouble talking to the Liqid Director
     */
    private void displayDevices(
        final Group group
    ) throws LiqidException {
        System.out.println();
        if (group == null) {
            System.out.println("All Resources:");
            System.out.println("  ---TYPE---  --NAME--  ----ID----  --------VENDOR--------  -----MODEL------  "
                                   + "-------MACHINE--------  -------------GROUP--------------  --DESCRIPTION--");
        } else {
            System.out.printf("Resources for group '%s':\n", group.getGroupName());
            System.out.println("  ---TYPE---  --NAME--  ----ID----  --------VENDOR--------  -----MODEL------  "
                                   + "-------MACHINE--------  --DESCRIPTION--");
        }

        for (var ds : _deviceStatusByName.values()) {
            var di = _deviceInfoById.get(ds.getDeviceId());
            var str1 = String.format("%-10s  %-8s  0x%08x  %-22s  %-16s",
                                     ds.getDeviceType(),
                                     ds.getName(),
                                     ds.getDeviceId(),
                                     di.getVendor(),
                                     di.getModel());

            var dr = _deviceRelationsByDeviceId.get(ds.getDeviceId());
            var machStr = "<none>";
            if (dr._machineId != null) {
                machStr = _machinesById.get(dr._machineId).getMachineName();
            }

            var grpStr = "";
            if (group == null) {
                var temp = (dr._groupId == null) ? "<none>" : _groupsById.get(dr._groupId).getGroupName();
                grpStr = String.format("  %-32s", temp);
            }

            System.out.printf("  %s  %-22s%s  %s\n", str1, machStr, grpStr, di.getUserDescription());
        }
    }

    /**
     * Displays machines
     * @param group Reference to Group if we want to limit the display to machines in that group, else null
     * @throws LiqidException If we have trouble talking to the Liqid Director
     */
    private void displayMachines(
        final Group group
    ) throws LiqidException {
        System.out.println();
        if (group == null) {
            System.out.println("All Machines:");
            System.out.println("  -------------GROUP--------------  -------MACHINE--------  ----ID----  --------DEVICES---------");
        } else {
            System.out.printf("Machines for group '%s':\n", group.getGroupName());
            System.out.println("  -------MACHINE--------  ----ID----  --------DEVICES---------");
        }

        for (var mach : _machinesById.values()) {
            var devNames = _deviceStatusByMachineId.get(mach.getMachineId())
                                                   .stream()
                                                   .map(DeviceStatus::getName)
                                                   .collect(Collectors.toCollection(TreeSet::new));
            var devNamesStr = String.join(" ", devNames);

            if (group == null) {
                var grp = _groupsById.get(mach.getGroupId());
                System.out.printf("  %-32s  %-22s  0x%08x  %s\n",
                                  grp.getGroupName(),
                                  mach.getMachineName(),
                                  mach.getMachineId(),
                                  devNamesStr);
            } else {
                System.out.printf("  %-22s  0x%08x  %s\n",
                                  mach.getMachineName(),
                                  mach.getMachineId(),
                                  devNamesStr);
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

        getLiqidInventory();
        var groupParam = _allFlag ? null : _groupsByName.get(_liqidGroupName);
        displayDevices(groupParam);
        displayMachines(groupParam);

        // All done
        logoutFromLiqidCluster();
        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
