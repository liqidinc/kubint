/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;

import java.util.Objects;

/**
 * Collects all the known information about a device into one convenient object
 */
public class DeviceItem {

    // The following are static.
    private final DeviceStatus _status;
    private final GeneralType _generalType;
    private final DeviceInfo _info;

    // The following can change as the device is attached and unattached to and from groups and machines.
    // A null value indicates no attachment.
    // A device can be attached to a group and not a machine, but if it is attached to a machine it *must* be
    // attached to the group which contains the machine.
    private Integer _groupId = null;
    private Integer _machineId = null;

    public DeviceItem(
        final DeviceStatus status,
        final DeviceInfo info
    ) {
        _status = status;
        _info = info;
        _generalType = GeneralType.fromDeviceType(status.getDeviceType());
    }

    //  shallow copy
    public DeviceItem copy() {
        var di = new DeviceItem(_status, _info);
        di._groupId = _groupId;
        di._machineId = _machineId;
        return di;
    }

    public DeviceStatus getDeviceStatus() { return _status; }
    public DeviceInfo getDeviceInfo() { return _info; }
    public GeneralType getGeneralType() { return _generalType; }
    public Integer getGroupId() { return _groupId; }
    public Integer getMachineId() { return _machineId; }
    public Integer getDeviceId() { return _status.getDeviceId(); }
    public String getDeviceName() { return _status.getName(); }

    public boolean isAssignedToGroup() { return _groupId != null; }
    public boolean isAssignedToMachine() { return _machineId != null; }

    public DeviceItem setGroupId(final Integer value) {_groupId = value; return this; }
    public DeviceItem setMachineId(final Integer value) {_machineId = value; return this; }

    @Override
    public boolean equals(
        final Object obj
    ) {
        if (obj instanceof DeviceItem di) {
            return Objects.equals(di.getMachineId(), getMachineId());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getDeviceId();
    }

    @Override
    public String toString() {
        return String.format("{id:0x%08X name:%s type:%s vendor:%s model:%s}",
                             getDeviceId(),
                             getDeviceName(),
                             getGeneralType(),
                             getDeviceInfo().getVendor(),
                             getDeviceInfo().getModel());
    }
}
