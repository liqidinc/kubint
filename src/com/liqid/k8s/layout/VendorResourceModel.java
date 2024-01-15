/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;

/**
 * Represents resource models of a particular general type and vendor.
 * For example, all models of NVidia GPUs.
 */
public class VendorResourceModel extends GenericResourceModel {

    private final String _vendorName;

    @Override
    public boolean accepts(
        final DeviceInfo deviceInfo
    ) {
        return super.accepts(deviceInfo) && _vendorName.equals(deviceInfo.getVendor());
    }

    public VendorResourceModel(
        final GeneralType generalType,
        final String vendorName
    ) {
        super(generalType);
        _vendorName = vendorName;
    }

    @Override
    public ResourceModelType getResourceModelType() {
        return ResourceModelType.B_VENDOR;
    }

    @Override
    public final String getVendorName() {
        return _vendorName;
    }
}
