/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

/**
 * Represents all device vendors and models of a particular general type.
 * For example, all GPU vendor/model combinations.
 */
public class GenericResourceModel extends ResourceModel {

    protected final GeneralType _generalType;

    public GenericResourceModel(
        final GeneralType generalType
    ) {
        _generalType = generalType;
    }

    @Override
    public final GeneralType getGeneralType() {
        return _generalType;
    }

    @Override
    public String getModelName() {
        return null;
    }

    @Override
    public ResourceModelType getResourceModelType() {
        return ResourceModelType.C_GENERIC;
    }

    @Override
    public String getVendorName() {
        return null;
    }
}
