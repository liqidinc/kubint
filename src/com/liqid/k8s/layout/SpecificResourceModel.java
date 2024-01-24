/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;

public final class SpecificResourceModel extends VendorResourceModel {

    private final String _modelName;

    @Override
    public boolean accepts(
        final DeviceInfo deviceInfo
    ) {
        return super.accepts(deviceInfo) && _modelName.equals(deviceInfo.getModel());
    }

    public SpecificResourceModel(
        final GeneralType generalType,
        final String vendorName,
        final String modelName
    ) {
        super(generalType, vendorName);
        _modelName = modelName;
    }

    public SpecificResourceModel(
        final DeviceInfo devInfo
    ) {
        this(GeneralType.fromDeviceType(devInfo.getDeviceInfoType()), devInfo.getVendor(), devInfo.getModel());
    }

    @Override
    public String getModelName() {
        return _modelName;
    }

    @Override
    public boolean isMoreSpecificThan(
        final ResourceModel resModel
    ) {
        return !(resModel instanceof SpecificResourceModel);
    }

    @Override
    public ResourceModelType getResourceModelType() {
        return ResourceModelType.A_SPECIFIC;
    }

    @Override
    public boolean overlaps(
        final ResourceModel other
    ) {
        return switch (other.getResourceModelType()) {
            case A_SPECIFIC -> this.getGeneralType().equals(other.getGeneralType())
                               && this.getVendorName().equals(other.getVendorName())
                               && this.getModelName().equals(other.getModelName());
            case B_VENDOR, C_GENERIC -> super.overlaps(other);
        };
    }
}
