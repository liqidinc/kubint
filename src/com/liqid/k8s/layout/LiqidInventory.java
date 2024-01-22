/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidException;
import com.liqid.sdk.Machine;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Caches the current state of the Liqid Inventory so that existence of and relationships between
 * the various Liqid resources can be polled for whatever purposes are necessary.
 * Be aware that this will go out of date, potentially in drastic ways, when other code makes changes
 * to the Liqid Configuration. Use the notify* methods to stay updated, else reload after bulk changes.
 */
public class LiqidInventory {

    // Inventory and quick look-up tables
    private final Map<Integer, DeviceItem> _deviceItems = new HashMap<>();
    private final Map<String, Integer> _deviceIdsByName = new HashMap<>();
    private final Map<Integer, Group> _groups = new HashMap<>();
    private final Map<String, Integer> _groupIdsByName = new HashMap<>();
    private final Map<Integer, Machine> _machines = new HashMap<>();
    private final Map<String, Integer> _machineIdsByName = new HashMap<>();

    /**
     * Default constructor
     */
    public LiqidInventory() {}

    /**
     * Creates a LiqidInventory object and populates it from the Liqid Cluster represented by the Liqid SDK
     * LiqidClient object
     * @param client LiqidClient object
     * @return The newly-created LiqidInventory object
     * @throws LiqidException If anything goes wrong while communicating with the Liqid Cluster
     */
    public static LiqidInventory createLiqidInventory(
        final LiqidClient client
    ) throws LiqidException {
        var inv = new LiqidInventory();

        // get all the DeviceStatus objects
        var devStats = client.getAllDevicesStatus();

        // get all the DeviceInfo objects
        var devInfos = new LinkedList<DeviceInfo>();
        devInfos.addAll(client.getComputeDeviceInfo());
        devInfos.addAll(client.getFPGADeviceInfo());
        devInfos.addAll(client.getGPUDeviceInfo());
        devInfos.addAll(client.getMemoryDeviceInfo());
        devInfos.addAll(client.getNetworkDeviceInfo());
        devInfos.addAll(client.getStorageDeviceInfo());
        var infoMap = devInfos.stream()
                              .collect(Collectors.toMap(DeviceInfo::getDeviceIdentifier, di -> di, (a, b) -> b, HashMap::new));

        // now build the inventory
        for (var ds : devStats) {
            inv._deviceItems.put(ds.getDeviceId(), new DeviceItem(ds, infoMap.get(ds.getDeviceId())));
            inv._deviceIdsByName.put(ds.getName(), ds.getDeviceId());
        }

        var groups = client.getGroups();
        for (var g : groups) {
            inv._groups.put(g.getGroupId(), g);
            inv._groupIdsByName.put(g.getGroupName(), g.getGroupId());

            for (var rel : client.getPreDevices(null, g.getGroupId(), null)) {
                var devId = inv._deviceIdsByName.get(rel.getDeviceName());
                inv._deviceItems.get(devId).setGroupId(g.getGroupId());
            }
        }

        var machines = client.getMachines();
        for (var m : machines) {
            inv._machines.put(m.getMachineId(), m);
            inv._machineIdsByName.put(m.getMachineName(), m.getMachineId());

            for (var rel : client.getPreDevices(null, m.getGroupId(), m.getMachineId())) {
                var devId = inv._deviceIdsByName.get(rel.getDeviceName());
                inv._deviceItems.get(devId).setMachineId(m.getMachineId());
            }
        }

        return inv;
    }

    /**
     * Creates a shallow-ish copy of this object.
     * Liqid SDK objects don't need to be copied; the reference can be moved over intact.
     * However, DeviceItem objects *do* need to be copied at their first level.
     * @return LiqidInventory object which is a copy of this object
     */
    public LiqidInventory copy() {
        var newInv = new LiqidInventory();
        _deviceItems.forEach((key, value) -> newInv._deviceItems.put(key, value.copy()));
        newInv._deviceIdsByName.putAll(_deviceIdsByName);
        newInv._groups.putAll(_groups);
        newInv._groupIdsByName.putAll(_groupIdsByName);
        newInv._machines.putAll(_machines);
        newInv._machineIdsByName.putAll(_machineIdsByName);
        return newInv;
    }

    /**
     * Finds the first compute device resource assigned to a particular machine.
     * There should actually only be one.
     * We *could* get this from the SDK Machine object by name, but I think this way is more reliable/resilient.
     * @param machineId machine identifier
     * @return DeviceItem for the compute device if it exists, else null
     */
    public DeviceItem getComputeDeviceItemForMachine(
        final Integer machineId
    ) {
        return getDeviceItemsForMachine(machineId).stream()
                                                  .filter(di -> di.getGeneralType() == GeneralType.CPU)
                                                  .findFirst()
                                                  .orElse(null);
    }

    /**
     * Retrieves the DeviceItem object for a particular device
     * @param deviceId device identifier
     * @return DeviceItem object if the device exists, else null
     */
    public DeviceItem getDeviceItem(
        final Integer deviceId
    ) {
        return _deviceItems.get(deviceId);
    }

    /**
     * Retrieves the DeviceItem object for a particular device
     * @param deviceName device name
     * @return DeviceItem object if the device exists, else null
     */
    public DeviceItem getDeviceItem(
        final String deviceName
    ) {
        return _deviceItems.get(_deviceIdsByName.get(deviceName));
    }

    /**
     * Retrieves a collection of all the DeviceItem objects in the inventory
     * @return collection of DeviceItem objects
     */
    public Collection<DeviceItem> getDeviceItems() {
        return new HashSet<>(_deviceItems.values());
    }

    /**
     * Retrieves a collection of DeviceItem objects for all the devices in a particular group
     * whether attached to machines or not.
     * @param groupId group identifier
     * @return collection of DeviceItem objects
     */
    public Collection<DeviceItem> getDeviceItemsForGroup(
        final Integer groupId
    ) {
        return _deviceItems.values()
                           .stream()
                           .filter(d -> Objects.equals(d.getGroupId(), groupId))
                           .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Retrieves a collection of DeviceItem objects for all the devices in a particular group
     * whether attached to machines or not.
     * @param groupName group name
     * @return collection of DeviceItem objects
     */
    public Collection<DeviceItem> getDeviceItemsForGroup(
        final String groupName
    ) {
        var gid = _groupIdsByName.get(groupName);
        return _deviceItems.values()
                           .stream()
                           .filter(d -> Objects.equals(d.getGroupId(), gid))
                           .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Retrieves a collection of DeviceItem objects for all the devices attached to a particular machine.
     * @param machineId machine identifier
     * @return collection of DeviceItem objects
     */
    public Collection<DeviceItem> getDeviceItemsForMachine(
        final Integer machineId
    ) {
        return _deviceItems.values()
                           .stream()
                           .filter(d -> Objects.equals(d.getMachineId(), machineId))
                           .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Retrieves a collection of DeviceItem objects for all the devices attached to a particular machine.
     * @param machineName machine name
     * @return collection of DeviceItem objects
     */
    public Collection<DeviceItem> getDeviceItemsForMachine(
        final String machineName
    ) {
        var mid = _groupIdsByName.get(machineName);
        return _deviceItems.values()
                           .stream()
                           .filter(d -> Objects.equals(d.getMachineId(), mid))
                           .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Given a collection of device identifiers, we construct a collection of corresponding device names
     * @param deviceIds device identifiers
     * @return collection of device names
     */
    public Set<String> getDeviceNamesFromIds(
        final Collection<Integer> deviceIds
    ) {
        return deviceIds.stream()
                        .map(id -> getDeviceItem(id).getDeviceName())
                        .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Retrieves the SDK Group object for a particular group
     * @param groupId group identifier
     * @return SDK Group object if the group exists, else null
     */
    public Group getGroup(
        final Integer groupId
    ) {
        return _groups.get(groupId);
    }

    /**
     * Retrieves the SDK Group object for a particular group
     * @param groupName group name
     * @return SDK Group object if the group exists, else null
     */
    public Group getGroup(
        final String groupName
    ) {
        return _groups.get(_groupIdsByName.get(groupName));
    }

    /**
     * Retrieves the group identifier of a particular group.
     * @param groupName group name
     * @return identifier of the group if it exists, else null
     */
    public Integer getGroupId(
        final String groupName
    ) {
        return _groupIdsByName.get(groupName);
    }

    /**
     * Retrieves the identifier of the group which contains a particular device.
     * @param deviceId device identifier
     * @return group identifier if the device is attached to a group, else null
     */
    public Integer getGroupIdForDevice(
        final Integer deviceId
    ) {
        return _deviceItems.get(deviceId).getGroupId();
    }

    /**
     * Retrieves the identifier of the group which contains a particular device.
     * @param deviceName device name
     * @return group identifier if the device is attached to a group, else null
     */
    public Integer getGroupIdForDevice(
        final String deviceName
    ) {
        return _deviceItems.get(_deviceIdsByName.get(deviceName)).getGroupId();
    }

    /**
     * Retrieves a collection of the SDK Group objects for all the groups in the inventory.
     * @return Group object if found, else null
     */
    public Collection<Group> getGroups() {
        return new HashSet<>(_groups.values());
    }

    /**
     * Returns the name of the k8s worker node associated with the indicated compute resource
     * represented by the given device item.
     * This is basically just the user description for the device.
     * @param deviceItem device item of interest
     * @return node name
     */
    public String getK8sNodeNameFromComputeDeviceItem(
        final DeviceItem deviceItem
    ) {
        return deviceItem.getDeviceInfo().getUserDescription();
    }

    /**
     * Returns the name of the k8s worker node associated with the indicated machine.
     * This is basically just the user description for the compute device associated with the machine.
     * @param machineId machine identifier
     * @return node name, or null if something is wrong in the configuration
     */
    public String getK8sNodeNameFromMachine(
        final Integer machineId
    ) {
        var compItem = getComputeDeviceItemForMachine(machineId);
        return compItem != null ? getK8sNodeNameFromComputeDeviceItem(compItem) : null;
    }

    /**
     * Retrieves the SDK Machine object for a particular machine
     * @param machineId machine identifier
     * @return Machine object if found, else null
     */
    public Machine getMachine(
        final Integer machineId
    ) {
        return _machines.get(machineId);
    }

    /**
     * Retrieves the SDK Machine object for a particular machine
     * @param machineName machine name
     * @return Machine object if found, else null
     */
    public Machine getMachine(
        final String machineName
    ) {
        return _machines.get(_machineIdsByName.get(machineName));
    }

    /**
     * Retrieves the machine identifier of a particular machine.
     * @param machineName machine name
     * @return identifier of the machine if it exists, else null
     */
    public Integer getMachineId(
        final String machineName
    ) {
        return _machineIdsByName.get(machineName);
    }

    /**
     * Retrieves the machine identifier for the machine to which a given device is attached
     * @param deviceId identifier of the device
     * @return machine identifier, or null if the device is not attached to any machine
     */
    public Integer getMachineIdForDevice(
        final Integer deviceId
    ) {
        return _deviceItems.get(deviceId).getMachineId();
    }

    /**
     * Retrieves the machine identifier for the machine to which a given device is attached
     * @param deviceName name of the device
     * @return machine identifier, or null if the device is not attached to any machine
     */
    public Integer getMachineIdForDevice(
        final String deviceName
    ) {
        return _deviceItems.get(_deviceIdsByName.get(deviceName)).getMachineId();
    }

    /**
     * Retrieves a collection of SDK Machine objects for all the configured machines.
     * @return collection of SDK Machine objects
     */
    public Collection<Machine> getMachines() {
        return new HashSet<>(_machines.values());
    }

    /**
     * Retrieves a collection of SDK Machine objects for all machines in a given group
     * @param groupId group identifier of the group
     * @return collection of SDK Machine objects
     */
    public Collection<Machine> getMachinesInGroup(
        final Integer groupId
    ) {
        return _machines.values()
                        .stream()
                        .filter(m -> Objects.equals(m.getGroupId(), groupId))
                        .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Retrieves a collection of machine identifiers of all the machines in a given group
     * @param groupId group identifier of the group
     * @return collection of machine identifiers
     */
    public Collection<Integer> getMachineIdsInGroup(
        final Integer groupId
    ) {
        return _machines.values()
                        .stream()
                        .filter(m -> Objects.equals(m.getGroupId(), groupId))
                        .map(Machine::getMachineId).collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Returns true if the inventory has at least once device matching the given vendor and model names.
     */
    public boolean hasDevice(
        final String vendor,
        final String model
    ) {
        return _deviceItems.values()
                           .stream()
                           .anyMatch(di -> di.getDeviceInfo().getVendor().equals(vendor)
                                           && di.getDeviceInfo().getModel().equals(model));
    }

    /**
     * Returns true if the inventory has at least one device matching the given model name.
     */
    public boolean hasDevice(
        final String model
    ) {
        return _deviceItems.values().stream().anyMatch(di -> di.getDeviceInfo().getModel().equals(model));
    }

    /**
     * Invoke to notify the inventory that a device was assigned to a group.
     * Used to avoid reloading the entire inventory.
     */
    public void notifyDeviceAssignedToGroup(
        final Integer deviceId,
        final Integer groupId
    ) {
        _deviceItems.get(deviceId).setGroupId(groupId);
        _deviceItems.get(deviceId).setMachineId(null);
    }

    /**
     * Invoke to notify the inventory that a device was assigned to a machine.
     * Used to avoid reloading the entire inventory.
     */
    public void notifyDeviceAssignedToMachine(
        final Integer deviceId,
        final Integer machineId
    ) {
        var groupId = _machines.get(machineId).getGroupId();
        _deviceItems.get(deviceId).setGroupId(groupId);
        _deviceItems.get(deviceId).setMachineId(machineId);
    }

    /**
     * Invoke to notify the inventory that a device was created (or at least, was defined).
     * Used to avoid reloading the entire inventory.
     */
    public void notifyDeviceCreated(
        final DeviceStatus status,
        final DeviceInfo info
    ) {
        var item = new DeviceItem(status, info);
        _deviceItems.put(status.getDeviceId(), item);
        _deviceIdsByName.put(status.getName(), status.getDeviceId());
    }

    /**
     * Invoke to notify the inventory that a device no longer exists.
     * Used to avoid reloading the entire inventory.
     */
    public void notifyDeviceRemoved(
        final Integer deviceId
    ) {
        if (_deviceItems.containsKey(deviceId)) {
            var di = _deviceItems.get(deviceId);
            _deviceIdsByName.remove(di.getDeviceStatus().getName());
            _deviceItems.remove(deviceId);
        }
    }

    /**
     * Invoke to notify the inventory that a device has been removed from its containing group.
     * Used to avoid reloading the entire inventory.
     */
    public void notifyDeviceRemovedFromGroup(
        final Integer deviceId
    ) {
        _deviceItems.get(deviceId).setGroupId(null);
        _deviceItems.get(deviceId).setMachineId(null);
    }

    /**
     * Invoke to notify the inventory that a device has been removed from its containing machine.
     * Used to avoid reloading the entire inventory.
     */
    public void notifyDeviceRemovedFromMachine(
        final Integer deviceId
    ) {
        _deviceItems.get(deviceId).setMachineId(null);
    }

    /**
     * Invoke to notify the inventory that a group was created.
     * Used to avoid reloading the entire inventory.
     */
    public void notifyGroupCreated(
        final Group group
    ) {
        _groups.put(group.getGroupId(), group);
    }

    /**
     * Invoke to notify the inventory that a group was removed.
     * Used to avoid reloading the entire inventory.
     */
    public void notifyGroupRemoved(
        final Integer groupId
    ) {
        if (_groups.containsKey(groupId)) {
            var g = _groups.get(groupId);
            _groupIdsByName.remove(g.getGroupName());
            _groups.remove(groupId);
        }
    }

    /**
     * Invoke to notify the inventory that a machine was created.
     * Used to avoid reloading the entire inventory.
     */
    public void notifyMachineCreated(
        final Machine machine
    ) {
        _machines.put(machine.getMachineId(), machine);
        _machineIdsByName.put(machine.getMachineName(), machine.getMachineId());
    }

    /**
     * Invoke to notify the inventory that a machine was removed.
     * Used to avoid reloading the entire inventory.
     */
    public void notifyMachineRemoved(
        final Integer machineId
    ) {
        if (_machines.containsKey(machineId)) {
            var m = _machines.get(machineId);
            _machineIdsByName.remove(m.getMachineName());
            _machines.remove(machineId);
        }
    }

    //  ----------------------------------------------------------------------------------------------------------------

    /**
     * Given a collection of DeviceItem objects, we construct a collection of corresponding device names
     * @param deviceItems device items
     * @return collection of device names
     */
    public static Set<String> getDeviceNamesFromItems(
        final Collection<DeviceItem> deviceItems
    ) {
        return deviceItems.stream()
                          .map(DeviceItem::getDeviceName)
                          .collect(Collectors.toCollection(HashSet::new));
    }

    public static void removeDeviceItemsOfType(
        final Collection<DeviceItem> deviceItems,
        final GeneralType generalType
    ) {
        deviceItems.removeIf(di -> di.getGeneralType() == generalType);
    }

    public static void removeDeviceItemsNotOfType(
        final Collection<DeviceItem> deviceItems,
        final GeneralType generalType
    ) {
        deviceItems.removeIf(di -> di.getGeneralType() != generalType);
    }

    /**
     * Removes all of the DeviceItem objects for devices which are not in a particular group.
     * @param deviceItems collection to be pruned
     * @param groupId group id of the group in quest
     */
    public static void removeDeviceItemsNotInGroup(
        final Collection<DeviceItem> deviceItems,
        final Integer groupId
    ) {
        deviceItems.removeIf(di -> !Objects.equals(di.getGroupId(), groupId));
    }

    /**
     * Removes all of the DeviceItem objects for devices which are attached to any machine.
     * @param deviceItems collection to be pruned
     */
    public static void removeDeviceItemsInAnyMachine(
        final Collection<DeviceItem> deviceItems
    ) {
        deviceItems.removeIf(di -> di.getMachineId() != null);
    }

    /**
     * Creates a map keyed by general device type, where each value is all of the DeviceItem objects of that type.
     * @param deviceItems a flat collection of DeviceItem objects
     */
    public static Map<GeneralType, LinkedList<DeviceItem>> segregateDeviceItemsByType(
        final Collection<DeviceItem> deviceItems
    ) {
        var result = new HashMap<GeneralType, LinkedList<DeviceItem>>();
        for (var devItem : deviceItems) {
            result.computeIfAbsent(devItem.getGeneralType(), k -> new LinkedList<>());
            result.get(devItem.getGeneralType()).add(devItem);
        }
        return result;
    }
}
