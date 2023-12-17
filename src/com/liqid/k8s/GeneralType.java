/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

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
public enum GeneralType {
    CPU,
    FPGA,
    GPU,
    LINK,
    MEMORY,
    SSD;

    private static final Map<DeviceType, GeneralType> TYPE_CONVERSION_MAP = new HashMap<>();
    static {
        TYPE_CONVERSION_MAP.put(DeviceType.COMPUTE, GeneralType.CPU);
        TYPE_CONVERSION_MAP.put(DeviceType.FPGA, GeneralType.FPGA);
        TYPE_CONVERSION_MAP.put(DeviceType.GPU, GeneralType.GPU);
        TYPE_CONVERSION_MAP.put(DeviceType.ETHERNET_LINK, GeneralType.LINK);
        TYPE_CONVERSION_MAP.put(DeviceType.FIBER_CHANNEL_LINK, GeneralType.LINK);
        TYPE_CONVERSION_MAP.put(DeviceType.INFINIBAND_LINK, GeneralType.LINK);
        TYPE_CONVERSION_MAP.put(DeviceType.MEMORY, GeneralType.MEMORY);
        TYPE_CONVERSION_MAP.put(DeviceType.SSD, GeneralType.SSD);
    }

    public static GeneralType fromDeviceType(
        final DeviceType devType
    ) {
        return TYPE_CONVERSION_MAP.get(devType);
    }
}
