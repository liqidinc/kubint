/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;

import java.util.Objects;

public class ResourceModel {

    private final LiqidGeneralType _generalType;
    private final String _vendorName;
    private final String _modelName;

    public ResourceModel(
        final LiqidGeneralType generalType,
        final String vendorName,
        final String modelName
    ) {
        _generalType = generalType;
        _vendorName = vendorName;
        _modelName = modelName;
    }

    /**
     * Constructor for generic resource - no vendor or model name
     */
    public ResourceModel(
        final LiqidGeneralType generalType
    ) {
        _generalType = generalType;
        _vendorName = null;
        _modelName = null;
    }

    public ResourceModel(
        final DeviceInfo devInfo
    ) {
        _generalType = LiqidGeneralType.fromDeviceType(devInfo.getDeviceInfoType());
        _vendorName = devInfo.getVendor();
        _modelName = devInfo.getModel();
    }

    public LiqidGeneralType getGeneralType() {
        return _generalType;
    }

    public String getVendorName() {
        return _vendorName;
    }

    public String getModelName() {
        return _modelName;
    }

    public boolean isGeneric() {
        return (_vendorName == null) && (_modelName == null);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ResourceModel rm) {
            return rm._generalType.equals(_generalType)
                   && Objects.equals(rm._vendorName, _vendorName)
                   && Objects.equals(rm._modelName, _modelName);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return _generalType.hashCode()
               ^ ((_vendorName == null) ? 0 : _vendorName.hashCode())
               ^ ((_modelName == null) ? 0 : _modelName.hashCode());
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append(_generalType.toString());
        if (_vendorName == null || _modelName == null) {
            sb.append('[').append("*").append(']');
        } else {
            sb.append('[').append(_vendorName).append(":").append(_modelName).append(']');
        }
        return sb.toString();
    }
}
