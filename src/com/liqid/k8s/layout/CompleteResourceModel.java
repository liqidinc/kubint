/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;

import java.util.Objects;

public class CompleteResourceModel extends VendorResourceModel {

    private final String _modelName;

    public CompleteResourceModel(
        final GeneralType generalType,
        final String vendorName,
        final String modelName
    ) {
        super(generalType, vendorName);
        _modelName = modelName;
    }

    public CompleteResourceModel(
        final DeviceInfo devInfo
    ) {
        this(GeneralType.fromDeviceType(devInfo.getDeviceInfoType()), devInfo.getVendor(), devInfo.getModel());
    }

    @Override
    public String getModelName() {
        return _modelName;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof CompleteResourceModel rm) {
            return rm._generalType.equals(_generalType)
                   && Objects.equals(rm._vendorName, _vendorName)
                   && Objects.equals(rm._modelName, _modelName);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return _generalType.hashCode() ^ _vendorName.hashCode() ^ _modelName.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s[%s:%s]", _generalType.toString(), _vendorName, _modelName);
    }
}
