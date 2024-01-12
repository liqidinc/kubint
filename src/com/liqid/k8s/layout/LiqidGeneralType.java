/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceType;

import java.util.HashMap;
import java.util.Map;

/**
 * This nonsense is made necessary because we want to effectively treat all LINK devices the same...
 * Well, not really, but really. We're not so much advocating treating fibre-channel HBAs the same as
 * ethernet HBAs, although that will be the practical outcome.  We just don't want to have to have to deal with
 * all the various sub-types of resources separately, nor do we wish to subject our users to that.
 * It's very unlikely we will run into configurations with different types of links, and if we do we can
 * let the user play games with vendors and models to sort things out.
 */
public enum LiqidGeneralType {
    CPU,
    FPGA,
    GPU,
    LINK,
    MEMORY,
    SSD;

    private static final Map<DeviceType, LiqidGeneralType> TYPE_CONVERSION_MAP = new HashMap<>();
    static {
        TYPE_CONVERSION_MAP.put(DeviceType.COMPUTE, LiqidGeneralType.CPU);
        TYPE_CONVERSION_MAP.put(DeviceType.FPGA, LiqidGeneralType.FPGA);
        TYPE_CONVERSION_MAP.put(DeviceType.GPU, LiqidGeneralType.GPU);
        TYPE_CONVERSION_MAP.put(DeviceType.ETHERNET_LINK, LiqidGeneralType.LINK);
        TYPE_CONVERSION_MAP.put(DeviceType.FIBER_CHANNEL_LINK, LiqidGeneralType.LINK);
        TYPE_CONVERSION_MAP.put(DeviceType.INFINIBAND_LINK, LiqidGeneralType.LINK);
        TYPE_CONVERSION_MAP.put(DeviceType.MEMORY, LiqidGeneralType.MEMORY);
        TYPE_CONVERSION_MAP.put(DeviceType.SSD, LiqidGeneralType.SSD);
    }

    public static DeviceType fromGeneralType(
        final LiqidGeneralType generalType
    ) {
        return TYPE_CONVERSION_MAP.entrySet()
                                  .stream()
                                  .filter(entry -> entry.getValue().equals(generalType))
                                  .findFirst()
                                  .map(Map.Entry::getKey)
                                  .orElse(null);
    }

    public static LiqidGeneralType fromDeviceType(
        final DeviceType devType
    ) {
        return TYPE_CONVERSION_MAP.get(devType);
    }
}
