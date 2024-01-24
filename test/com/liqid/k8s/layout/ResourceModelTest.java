/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceType;
import org.junit.Test;

import java.util.Arrays;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ResourceModelTest {

    private static final GenericResourceModel CPU = new GenericResourceModel(GeneralType.CPU);
    private static final GenericResourceModel FPGA = new GenericResourceModel(GeneralType.FPGA);
    private static final GenericResourceModel GPU = new GenericResourceModel(GeneralType.GPU);
    private static final GenericResourceModel LINK = new GenericResourceModel(GeneralType.LINK);
    private static final GenericResourceModel SSD = new GenericResourceModel(GeneralType.SSD);

    private static final String INTEL_NAME = "Intel";
    private static final VendorResourceModel INTEL_CPU = new VendorResourceModel(GeneralType.CPU, INTEL_NAME);
    private static final VendorResourceModel INTEL_GPU = new VendorResourceModel(GeneralType.GPU, INTEL_NAME);
    private static final SpecificResourceModel INTEL_8080 = new SpecificResourceModel(GeneralType.CPU, INTEL_NAME, "8080");
    private static final SpecificResourceModel INTEL_8085 = new SpecificResourceModel(GeneralType.CPU, INTEL_NAME, "8085");
    private static final SpecificResourceModel INTEL_8086 = new SpecificResourceModel(GeneralType.CPU, INTEL_NAME, "8086");
    private static final SpecificResourceModel INTEL_A770 = new SpecificResourceModel(GeneralType.GPU, INTEL_NAME, "A770");

    private static final String ZILOG_NAME = "Zilog";
    private static final VendorResourceModel ZILOG_CPU = new VendorResourceModel(GeneralType.CPU, ZILOG_NAME);
    private static final SpecificResourceModel ZILOG_Z80 = new SpecificResourceModel(GeneralType.CPU, ZILOG_NAME, "Z80");
    private static final SpecificResourceModel ZILOG_Z80a = new SpecificResourceModel(GeneralType.CPU, ZILOG_NAME, "Z80A");

    private static final String ZYLINK_NAME = "Zylink";
    private static final VendorResourceModel ZYLINK_FPGA = new VendorResourceModel(GeneralType.FPGA, ZYLINK_NAME);

    private static final String NVIDIA_NAME = "NVidia Corporation";
    private static final VendorResourceModel NVIDIA_GPU = new VendorResourceModel(GeneralType.GPU, NVIDIA_NAME);
    private static final SpecificResourceModel NVIDIA_A100 = new SpecificResourceModel(GeneralType.GPU, NVIDIA_NAME, "A100");
    private static final SpecificResourceModel NVIDIA_L40 = new SpecificResourceModel(GeneralType.GPU, NVIDIA_NAME, "L40");
    private static final SpecificResourceModel NVIDIA_P4 = new SpecificResourceModel(GeneralType.GPU, NVIDIA_NAME, "P4");

    private static final String LIQID_NAME = "Liqid, Inc.";
    private static final VendorResourceModel LIQID_SSD = new VendorResourceModel(GeneralType.SSD, LIQID_NAME);
    private static final SpecificResourceModel LIQID_LQD4500 = new SpecificResourceModel(GeneralType.SSD, LIQID_NAME, "LQD4500");
    private static final SpecificResourceModel LIQID_LQD4510 = new SpecificResourceModel(GeneralType.SSD, LIQID_NAME, "LQD4510");

    @Test
    public void equality() {
        assertEquals(new GenericResourceModel(GeneralType.GPU), GPU);
        assertEquals(new VendorResourceModel(GeneralType.SSD, LIQID_NAME), LIQID_SSD);
        assertEquals(new SpecificResourceModel(GeneralType.CPU, ZILOG_NAME, "Z80"), ZILOG_Z80);

        assertNotEquals(new GenericResourceModel(GeneralType.FPGA),
                        new VendorResourceModel(GeneralType.FPGA, NVIDIA_NAME));
        assertNotEquals(new GenericResourceModel(GeneralType.FPGA),
                        new SpecificResourceModel(GeneralType.FPGA, NVIDIA_NAME, "ZL1000"));
        assertNotEquals(new VendorResourceModel(GeneralType.FPGA, NVIDIA_NAME),
                        new SpecificResourceModel(GeneralType.FPGA, NVIDIA_NAME, "ZL1000"));

        assertNotEquals(new VendorResourceModel(GeneralType.FPGA, LIQID_NAME), LIQID_SSD);
        assertNotEquals(new SpecificResourceModel(GeneralType.CPU, INTEL_NAME, "Xyzzy"),
                        new SpecificResourceModel(GeneralType.LINK, INTEL_NAME, "Xyzzy"));
        assertNotEquals(new SpecificResourceModel(GeneralType.CPU, "Motorola", "M68020"),
                        new SpecificResourceModel(GeneralType.CPU, "Motorola", "M68030"));
    }

    @Test
    public void orderingAmongGenericResourceModels() {
        var set = new TreeSet<ResourceModel>();
        set.add(SSD);
        set.add(CPU);
        set.add(GPU);
        set.add(FPGA);

        var expArray = new ResourceModel[]{ CPU, FPGA, GPU, SSD };
        var expList = Arrays.stream(expArray).toList();
        var resultList = set.stream().toList();
        assertEquals(expList, resultList);
    }

    @Test
    public void orderingAmongVendorResourceModels() {
        var set = new TreeSet<ResourceModel>();
        set.add(LIQID_SSD);
        set.add(INTEL_GPU);
        set.add(NVIDIA_GPU);
        set.add(ZILOG_CPU);
        set.add(INTEL_CPU);

        var expArray = new ResourceModel[]{INTEL_CPU, ZILOG_CPU, INTEL_GPU, NVIDIA_GPU, LIQID_SSD};
        var expList = Arrays.stream(expArray).toList();
        var resultList = set.stream().toList();
        assertEquals(expList, resultList);
    }

    @Test
    public void orderingAmongSpecificResourceModels() {
        var set = new TreeSet<ResourceModel>();
        set.add(INTEL_A770);
        set.add(INTEL_8085);
        set.add(NVIDIA_A100);
        set.add(ZILOG_Z80a);
        set.add(ZILOG_Z80);
        set.add(NVIDIA_L40);
        set.add(INTEL_8080);
        set.add(LIQID_LQD4510);
        set.add(INTEL_8086);
        set.add(LIQID_LQD4500);
        set.add(NVIDIA_P4);

        var expArray = new ResourceModel[]{
            INTEL_8080, INTEL_8085, INTEL_8086, ZILOG_Z80, ZILOG_Z80a,
            INTEL_A770, NVIDIA_A100, NVIDIA_L40, NVIDIA_P4,
            LIQID_LQD4500, LIQID_LQD4510,
        };
        var expList = Arrays.stream(expArray).toList();
        var resultList = set.stream().toList();
        assertEquals(expList, resultList);
    }

    @Test
    public void orderingAmongResourceModelTypes() {
        var set = new TreeSet<ResourceModelType>();
        set.add(ResourceModelType.B_VENDOR);
        set.add(ResourceModelType.C_GENERIC);
        set.add(ResourceModelType.A_SPECIFIC);

        var expArray = new ResourceModelType[]{
            ResourceModelType.A_SPECIFIC,
            ResourceModelType.B_VENDOR,
            ResourceModelType.C_GENERIC
        };
        var expList = Arrays.stream(expArray).toList();
        var resultList = set.stream().toList();
        assertEquals(expList, resultList);
    }

    @Test
    public void completeOrdering() {
        var set = new TreeSet<ResourceModel>();
        set.add(INTEL_A770);
        set.add(INTEL_8085);
        set.add(FPGA);
        set.add(NVIDIA_A100);
        set.add(ZILOG_Z80a);
        set.add(NVIDIA_GPU);
        set.add(ZILOG_CPU);
        set.add(ZYLINK_FPGA);
        set.add(INTEL_CPU);
        set.add(ZILOG_Z80);
        set.add(NVIDIA_L40);
        set.add(SSD);
        set.add(CPU);
        set.add(INTEL_8080);
        set.add(LIQID_LQD4510);
        set.add(INTEL_8086);
        set.add(LIQID_LQD4500);
        set.add(NVIDIA_P4);
        set.add(LIQID_SSD);
        set.add(LINK);
        set.add(INTEL_GPU);
        set.add(GPU);

        var expArray = new ResourceModel[]{
            INTEL_8080, INTEL_8085, INTEL_8086,
            ZILOG_Z80, ZILOG_Z80a,
            INTEL_CPU, ZILOG_CPU, CPU,

            ZYLINK_FPGA, FPGA,

            INTEL_A770,
            NVIDIA_A100, NVIDIA_L40, NVIDIA_P4,
            INTEL_GPU, NVIDIA_GPU, GPU,

            LINK,

            LIQID_LQD4500, LIQID_LQD4510,
            LIQID_SSD, SSD,
        };
        var expList = Arrays.stream(expArray).toList();
        var resultList = set.stream().toList();
        assertEquals(expList, resultList);
    }

    @Test
    public void accepts() {
        var nvidiaA100Info = new DeviceInfo();
        nvidiaA100Info.setDeviceInfoType(DeviceType.GPU);
        nvidiaA100Info.setVendor(NVIDIA_A100.getVendorName());
        nvidiaA100Info.setModel(NVIDIA_A100.getModelName());

        var nvidiaL40Info = new DeviceInfo();
        nvidiaL40Info.setDeviceInfoType(DeviceType.GPU);
        nvidiaL40Info.setVendor(NVIDIA_L40.getVendorName());
        nvidiaL40Info.setModel(NVIDIA_L40.getModelName());

        var intelGPUInfo = new DeviceInfo();
        intelGPUInfo.setDeviceInfoType(DeviceType.GPU);
        intelGPUInfo.setVendor(INTEL_A770.getVendorName());
        intelGPUInfo.setModel(INTEL_A770.getModelName());

        var liqidSSDInfo = new DeviceInfo();
        liqidSSDInfo.setDeviceInfoType(DeviceType.SSD);
        liqidSSDInfo.setVendor(LIQID_LQD4500.getVendorName());
        liqidSSDInfo.setModel(LIQID_LQD4500.getModelName());

        assertTrue(NVIDIA_A100.accepts(nvidiaA100Info));
        assertFalse(NVIDIA_A100.accepts(nvidiaL40Info));
        assertFalse(NVIDIA_A100.accepts(intelGPUInfo));
        assertFalse(NVIDIA_A100.accepts(liqidSSDInfo));

        assertTrue(NVIDIA_GPU.accepts(nvidiaA100Info));
        assertTrue(NVIDIA_GPU.accepts(nvidiaL40Info));
        assertFalse(NVIDIA_GPU.accepts(intelGPUInfo));
        assertFalse(NVIDIA_GPU.accepts(liqidSSDInfo));

        assertTrue(GPU.accepts(nvidiaA100Info));
        assertTrue(GPU.accepts(nvidiaL40Info));
        assertTrue(GPU.accepts(intelGPUInfo));
        assertFalse(GPU.accepts(liqidSSDInfo));
    }

    @Test
    public void isMoreSpecifcThan() {
        var gpuGeneral = new GenericResourceModel(GeneralType.GPU);
        var gpuNVidia = new VendorResourceModel(GeneralType.GPU, "NVidia");
        var gpuNVidiaL40 = new SpecificResourceModel(GeneralType.GPU, "NVidia", "L40");

        assertFalse(gpuGeneral.isMoreSpecificThan(gpuGeneral));
        assertFalse(gpuGeneral.isMoreSpecificThan(gpuNVidia));
        assertFalse(gpuGeneral.isMoreSpecificThan(gpuNVidiaL40));
        assertTrue(gpuNVidia.isMoreSpecificThan(gpuGeneral));
        assertFalse(gpuNVidia.isMoreSpecificThan(gpuNVidia));
        assertFalse(gpuNVidia.isMoreSpecificThan(gpuNVidiaL40));
        assertTrue(gpuNVidiaL40.isMoreSpecificThan(gpuGeneral));
        assertTrue(gpuNVidiaL40.isMoreSpecificThan(gpuNVidia));
        assertFalse(gpuNVidiaL40.isMoreSpecificThan(gpuNVidiaL40));
    }

    @Test
    public void overlaps() {
        var gpuGeneral = new GenericResourceModel(GeneralType.GPU);
        var gpuLiqid = new VendorResourceModel(GeneralType.GPU, "Liqid");
        var gpuNVidia = new VendorResourceModel(GeneralType.GPU, "NVidia");

        var fpgaGeneral = new GenericResourceModel(GeneralType.FPGA);

        var gpuIntelA770 = new SpecificResourceModel(GeneralType.GPU, "Intel", "A770");
        var gpuNVidiaL40 = new SpecificResourceModel(GeneralType.GPU, "NVidia", "L40");

        // general <-> general
        assertTrue(gpuGeneral.overlaps(gpuGeneral));
        assertFalse(gpuGeneral.overlaps(fpgaGeneral));

        // vendor <-> vendor
        assertTrue(gpuLiqid.overlaps(gpuLiqid));
        assertFalse(gpuLiqid.overlaps(gpuNVidia));

        // specific <-> specific
        assertTrue(gpuIntelA770.overlaps(gpuIntelA770));
        assertFalse(gpuIntelA770.overlaps(gpuNVidiaL40));

        // general <-> vendor
        assertTrue(gpuGeneral.overlaps(gpuLiqid));
        assertTrue(gpuLiqid.overlaps(gpuGeneral));
        assertFalse(fpgaGeneral.overlaps(gpuLiqid));
        assertFalse(gpuLiqid.overlaps(fpgaGeneral));

        // vendor <-> specific
        assertTrue(gpuNVidia.overlaps(gpuNVidiaL40));
        assertTrue(gpuNVidiaL40.overlaps(gpuNVidia));
        assertFalse(gpuNVidia.overlaps(gpuIntelA770));
        assertFalse(gpuIntelA770.overlaps(gpuNVidia));

        // general <-> specific
        assertTrue(gpuGeneral.overlaps(gpuNVidiaL40));
        assertTrue(gpuNVidiaL40.overlaps(gpuGeneral));
        assertFalse(fpgaGeneral.overlaps(gpuIntelA770));
        assertFalse(gpuIntelA770.overlaps(fpgaGeneral));
    }
}
