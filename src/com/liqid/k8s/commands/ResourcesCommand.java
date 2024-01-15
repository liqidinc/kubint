/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.LiqidException;

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

        for (var devItem : _liqidInventory.getDeviceItems()) {
            var machStr = "<none>";
            if (devItem.getMachineId() != null) {
                machStr = _liqidInventory.getMachine(devItem.getMachineId()).getMachineName();
            }

            var grpStr = "<none>";
            if (devItem.getGroupId() != null) {
                grpStr = _liqidInventory.getGroup(devItem.getGroupId()).getGroupName();
            }

            System.out.printf("  %-10s  %-8s  0x%08x  %-22s  %-16s  %-22s  %-32s  %s\n",
                              devItem.getGeneralType(),
                              devItem.getDeviceName(),
                              devItem.getDeviceId(),
                              devItem.getDeviceInfo().getVendor(),
                              devItem.getDeviceInfo().getModel(),
                              machStr,
                              grpStr,
                              devItem.getDeviceInfo().getUserDescription());
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

        for (var mach : _liqidInventory.getMachines()) {
            var machDevs = _liqidInventory.getDeviceItemsForMachine(mach.getMachineId());
            var devNames = _liqidInventory.getDeviceNamesFromItems(machDevs);
            var devNamesStr = String.join(" ", devNames);
            var grp = _liqidInventory.getGroup(mach.getGroupId());
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
        displayDevices();
        displayMachines();

        _logger.trace("Exiting %s with null", fn);
        return null;
    }
}
