/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import java.util.Objects;

public class VendorResourceModel extends ResourceModel {

    protected final String _vendorName;

    public VendorResourceModel(
        final GeneralType generalType,
        final String vendorName
    ) {
        super(generalType);
        _vendorName = vendorName;
    }

    @Override
    public String getVendorName() {
        return _vendorName;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof VendorResourceModel rm) {
            return rm._generalType.equals(_generalType)
                   && Objects.equals(rm._vendorName, _vendorName);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return _generalType.hashCode() ^ _vendorName.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s[%s:*]", _generalType.toString(), _vendorName);
    }
}
