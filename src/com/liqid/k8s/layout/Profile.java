/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.k8s.LiqidGeneralType;
import com.liqid.sdk.DeviceInfo;

import java.util.HashMap;
import java.util.Map;

public class Profile {

    private final Map<LiqidGeneralType, TypeProfile> _profile = new HashMap<>();

    public void injectCount(
        final LiqidGeneralType generalType,
        final String vendorName,
        final String modelName,
        final Integer count
    ) {
        var resModel = new ResourceModel(generalType, vendorName, modelName);
        var tProf = _profile.get(generalType);
        if (tProf == null) {
            tProf = new TypeProfile();
            _profile.put(generalType, tProf);
        }

        tProf.put(resModel, count);
    }

    public void injectGenericCount(
        final LiqidGeneralType generalType,
        final Integer count
    ) {
        var resModel = new ResourceModel(generalType);
        var tProf = _profile.get(generalType);
        if (tProf == null) {
            tProf = new TypeProfile();
            _profile.put(generalType, tProf);
        }

        tProf.put(resModel, count);
    }

    public void injectDevice(
        final DeviceInfo devInfo
    ) {
        var resModel = new ResourceModel(devInfo);
        var gType = resModel.getGeneralType();
        var tProf = _profile.get(gType);
        if (tProf == null) {
            tProf = new TypeProfile();
            _profile.put(gType, tProf);
        }

        int newCount = tProf.getOrDefault(resModel, 0) + 1;
        tProf.put(resModel, newCount);
    }

    public void show(
        final String indent
    ) {
        for (var tp : _profile.values()) {
            tp.show(indent);
        }
    }
}
