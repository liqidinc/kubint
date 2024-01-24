/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;

import java.util.Objects;

public abstract class ResourceModel implements Comparable<ResourceModel> {

    public abstract ResourceModelType getResourceModelType();
    public abstract GeneralType getGeneralType();
    public abstract String getVendorName();
    public abstract String getModelName();

    /**
     * Tests a given SDK DeviceInfo object to determine whether this ResourceModel accurately
     * describes the corresponding device.
     * @param deviceInfo SDK DeviceInfo object
     * @return true if the device can be described by this ResourceModel object, else false
     */
    public abstract boolean accepts(final DeviceInfo deviceInfo);

    public abstract boolean isMoreSpecificThan(final ResourceModel resModel);

    /**
     * Tests whether this object and a similar object overlap...
     * We overlap if whichever data items this object has in common with the other, are equivalent.
     * Thus, a Generic would overlap a specific if the general types were equivalent,
     * while two specific objects would only overlap if the type, vendor, and model were all equivalent.
     * @param resourceModel other object to be tested
     * @return true if this object overlaps the other
     */
    public abstract boolean overlaps(final ResourceModel resourceModel);

    @Override
    public final boolean equals(final Object obj) {
        return (obj instanceof ResourceModel rm)
            && (getResourceModelType().equals(rm.getResourceModelType()))
            && (getGeneralType().equals(rm.getGeneralType()))
            && (Objects.equals(getVendorName(), rm.getVendorName()))
            && (Objects.equals(getModelName(), rm.getModelName()));
    }

    @Override
    public int hashCode() {
        int code = getResourceModelType().hashCode() ^ getGeneralType().hashCode();
        if (this instanceof VendorResourceModel) {
            code ^= getVendorName().hashCode();
        }
        return code;
    }

    /**
     * Provides ordering for the ResourceModel objects
     * @param compObject the object to be compared.
     * @return  -1 if this object sorts lower than (ahead of) the comparison object
     *          0 if this object sorts equally to the comparison object
     *          +1 if this object sorts greater than (behind) the comparison object
     */
    @Override
    public final int compareTo(final ResourceModel compObject) {
        int comp = getGeneralType().compareTo(compObject.getGeneralType());
        if (comp == 0) {
            // same general type - check resource model type.
            // The most specific should sort ahead of the least specific.
            comp = getResourceModelType().compareTo(compObject.getResourceModelType());
            if ((comp == 0) && (getVendorName() != null)) {
                comp = getVendorName().compareTo(compObject.getVendorName());
                if ((comp == 0) && (getModelName() != null)) {
                    comp = getModelName().compareTo(compObject.getModelName());
                }
            }
        }
        return comp;
    }

    @Override
    public final String toString() {
        var typeStr = getGeneralType();
        var vendorStr = getVendorName() == null ? "*" : getVendorName();
        var modelStr = getModelName() == null ? "*" : getModelName();
        return String.format("%s[%s:%s]", typeStr, vendorStr, modelStr);
    }
}
