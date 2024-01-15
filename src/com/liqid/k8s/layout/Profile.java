/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Contains a count of devices (which may be positive or negative) for each known
 * combination of type, vendor, and model.
 * A special case exists representing the generic type, where vendor and model are null.
 */
public class Profile {

    private final Map<ResourceModel, Integer> _content = new HashMap<>();

    Profile() {}

    /**
     * Retrieve the count for a particular ResourceModel.
     * @return the given count if we are tracking it, or null if we are not tracking
     */
    public Integer getCount(
        final ResourceModel key
    ) {
        return _content.get(key);
    }

    /**
     * Retrieve the set of all currently known ResourceModels
     */
    public Set<ResourceModel> getResourceModels() {
        return _content.keySet();
    }

    /**
     * Updates the current count for the given combination of type, vendor, and model as specified in a given
     * ResourceModel to account for an additional +/- number of devices.
     * If no entry exists yet for that combination, one is created and set to the count value.
     * @param resourceModel model of interest
     * @param count number of devices of this general type
     */
    public void injectCount(
        final ResourceModel resourceModel,
        final Integer count
    ) {
        var newCount = count;
        if (_content.containsKey(resourceModel)) {
            newCount += _content.get(resourceModel);
        }

        _content.put(resourceModel, newCount);
    }

    /**
     * Updates the count of devices which matches the given device info, respecting vendor name and model,
     * incrementing by one. If there is no entry yet for the given device type, vendor, and model, and entry
     * is created with a count of one.
     * @param devItem Liqid DeviceItem object
     */
    public void injectDevice(
        final DeviceItem devItem
    ) {
        injectCount(new CompleteResourceModel(devItem.getDeviceInfo()), 1);
    }

    /**
     * Given an existing profile, we inject the values from that profile into this one.
     * Effectively, it adds all the device counts from the given profile to this one, preserving the
     * type, vendor, and model specifications per type.
     * @param profile a different profile with counters to be injected into this one.
     */
    public void injectProfile(
        final Profile profile
    ) {
        for (var entry : profile._content.entrySet()) {
            var resModel = entry.getKey();
            var count = entry.getValue();
            injectCount(resModel, count);
        }
    }

    /**
     * Display content
     * @param indent assist with display formatting - this string is prepended to all output
     */
    public void show(
        final String indent
    ) {
        for (var e : _content.entrySet()) {
            System.out.printf("%s%s=%d\n", indent, e.getKey().toString(), e.getValue());
        }
    }
}
