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

    @Override
    public Plan process(
    ) throws InternalErrorException, LiqidException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

        initLiqidClient();
        getLiqidInventory();
        displayDevices(null);
        displayMachines(null);

        _logger.trace("Exiting %s with null", fn);
        return null;
    }
}
