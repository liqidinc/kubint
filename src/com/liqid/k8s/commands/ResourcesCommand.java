/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.LiqidException;

import java.util.TreeSet;
import java.util.stream.Collectors;

public class ResourcesCommand extends Command {

    public ResourcesCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public ResourcesCommand setLiqidAddress(final String value) {_liqidAddress = value; return this; }
    public ResourcesCommand setLiqidPassword(final String value) {_liqidPassword = value; return this; }
    public ResourcesCommand setLiqidUsername(final String value) {_liqidUsername = value; return this; }

    /**
     * Displays devices based on the current known liqid inventory (see getLiqidInventory())
     */
    protected void displayDevices() {
        var fn = "displayDevices";
        _logger.trace("Entering %s", fn);

        System.out.println();
        System.out.println("All Resources:");
        System.out.println("  ---TYPE---  --NAME--  ----ID----  --------VENDOR--------  -----MODEL------  "
                           + "-------MACHINE--------  -------------GROUP--------------  --DESCRIPTION--");

        for (var ds : _liqidInventory._deviceStatusByName.values()) {
            var di = _liqidInventory._deviceInfoById.get(ds.getDeviceId());
            var str1 = String.format("%-10s  %-8s  0x%08x  %-22s  %-16s",
                                     ds.getDeviceType(),
                                     ds.getName(),
                                     ds.getDeviceId(),
                                     di.getVendor(),
                                     di.getModel());

            var dr = _liqidInventory._deviceRelationsByDeviceId.get(ds.getDeviceId());
            var machStr = "<none>";
            if (dr._machineId != null) {
                machStr = _liqidInventory._machinesById.get(dr._machineId).getMachineName();
            }

            var grpStr = "";
            var temp = (dr._groupId == null)
                       ? "<none>"
                       : _liqidInventory._groupsById.get(dr._groupId).getGroupName();
            grpStr = String.format("  %-32s", temp);

            System.out.printf("  %s  %-22s%s  %s\n", str1, machStr, grpStr, di.getUserDescription());
        }

        _logger.trace("Exiting %s", fn);
    }

    /**
     * Displays machines based on the current known liqid inventory (see getLiqidInventory())
     */
    protected void displayMachines() {
        var fn = "displayMachines";
        _logger.trace("Entering %s", fn);

        System.out.println();
        System.out.println("All Machines:");
        System.out.println("  -------------GROUP--------------  -------MACHINE--------  ----ID----  --------DEVICES---------");

        for (var mach : _liqidInventory._machinesById.values()) {
            var devNames = _liqidInventory._deviceStatusByMachineId.get(mach.getMachineId())
                                                                   .stream()
                                                                   .map(DeviceStatus::getName)
                                                                   .collect(Collectors.toCollection(TreeSet::new));
            var devNamesStr = String.join(" ", devNames);
            var grp = _liqidInventory._groupsById.get(mach.getGroupId());
            System.out.printf("  %-32s  %-22s  0x%08x  %s\n",
                              grp.getGroupName(),
                              mach.getMachineName(),
                              mach.getMachineId(),
                              devNamesStr);
        }

        _logger.trace("Exiting %s", fn);
    }

    @Override
    public Plan process(
    ) throws InternalErrorException, LiqidException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

        initLiqidClient();
        getLiqidInventory();
        displayDevices();
        displayMachines();

        _logger.trace("Exiting %s with null", fn);
        return null;
    }
}
