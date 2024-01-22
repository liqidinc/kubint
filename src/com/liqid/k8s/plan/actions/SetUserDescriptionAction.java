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

public class SetUserDescriptionAction extends Action {

    private String _deviceName;
    private String _description;

    public SetUserDescriptionAction() {
        super(ActionType.SET_USER_DESCRIPTION);
    }

    public String getDescription() { return _description; }
    public String getDeviceName() { return _deviceName; }
    public SetUserDescriptionAction setDescription(final String value) {_description = value; return this; }
    public SetUserDescriptionAction setDeviceName(final String value) {_deviceName = value; return this; }

    @Override
    public void checkParameters() throws InternalErrorException {
        checkForNull("DeviceName", _deviceName);
        checkForNull("DeviceDescription", _description);
    }

    @Override
    public void perform(
        final ExecutionContext context
    ) throws LiqidException, ProcessingException {
        var fn = this.getClass().getName() + ":perform";
        context.getLogger().trace("Entering %s", fn);

        var devStat = context.getLiqidInventory().getDeviceItem(_deviceName).getDeviceStatus();
        var qType = LiqidClient.deviceTypeToQueryDeviceType(devStat.getDeviceType());
        context.getLiqidClient().createDeviceDescription(qType, devStat.getDeviceId(), _description);

        context.getLogger().trace("%s returning", fn);
    }

    @Override
    public String toString() {
        return String.format("Set User Description for Device %s to '%s'", _deviceName, _description);
    }
}
