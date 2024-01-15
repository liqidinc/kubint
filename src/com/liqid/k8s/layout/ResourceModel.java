/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

public class ResourceModel {

    protected final GeneralType _generalType;

    public ResourceModel(
        final GeneralType generalType
    ) {
        _generalType = generalType;
    }

    public GeneralType getGeneralType() { return _generalType; }
    public String getVendorName() { return null; }
    public String getModelName() { return null; }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ResourceModel rm) {
            return rm._generalType.equals(_generalType);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return _generalType.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s[*:*]", _generalType.toString());
    }
}
