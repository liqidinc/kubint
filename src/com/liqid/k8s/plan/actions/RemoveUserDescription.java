/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.ExecutionContext;
import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidException;

public class RemoveUserDescription extends Action {

    private String _deviceName;

    public RemoveUserDescription() {
        super(ActionType.SET_USER_DESCRIPTION);
    }

    public RemoveUserDescription setDeviceName(final String value) {_deviceName = value; return this; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("DeviceName", _deviceName);
    }

    @Override
    public void perform(
        final ExecutionContext context
    ) throws LiqidException, ProcessingException {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        var devStat = context.getLiqidInventory()._deviceStatusByName.get(_deviceName);
        var qType = LiqidClient.deviceTypeToQueryDeviceType(devStat.getDeviceType());
        context.getLiqidClient().deleteDeviceDescription(qType, devStat.getDeviceId());

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return String.format("Delete User Description for Device %s", _deviceName);
    }
}
