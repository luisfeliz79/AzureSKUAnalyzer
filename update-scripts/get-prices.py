#!/usr/bin/env python3
"""
get-prices.py
=============
By Luis Feliz
=============
Fetches Azure Virtual Machine retail pricing from the Azure Retail Prices API,
merges regional data into a single pivot table, enriches each SKU with parsed
hardware details (vCPUs, memory, features, etc.), and outputs a final CSV report.

Workflow:
  1. pull_data_from_api() – queries the API for each target region and saves per-region CSVs.
  2. merge_data()         – combines all regional CSVs into a pivoted "combined.csv".
  3. add_sku_details()    – parses SKU names and appends hardware metadata to produce
                            the final "sku-price-report-by-region.csv".
"""

import csv
from datetime import datetime
import requests
import json
import time

import glob
import os
import pandas as pd

import re

# from tabulate import tabulate  # optional – uncomment for pretty-printed console tables

# ---------------------------------------------------------------------------
# Load memory and processor lookups from series-info-scraped-from-docs.csv
# ---------------------------------------------------------------------------
_MEMORY_LOOKUP: dict[str, str] = {}
_MEMORY_LOOKUP2: dict[str, str] = {}
_MEMORY_LOOKUP3: dict[str, str] = {}
_MEMORY_LOOKUP4: dict[str, str] = {}
_PROCESSOR_LOOKUP: dict[str, str] = {}
_VCPU_LOOKUP: dict[str, int] = {}
_VCPU_LOOKUP2: dict[str, int] = {}
_CATEGORY_LOOKUP: dict[str, str] = {}
_DOCUMENT_PATH_LOOKUP: dict[str, str] = {}
_NETWORKBW_LOOKUP: dict[str, str] = {}
_NETWORKBW2_LOOKUP: dict[str, str] = {}
_NETWORKBW3_LOOKUP: dict[str, str] = {}
_NETWORKBW4_LOOKUP: dict[str, str] = {}

_LIVE_MIGRATION_LOOKUP: dict[str, str] = {}

_NESTED_VIRTUALIZATION_LOOKUP: dict[str, str] = {}


#_series_info_path = os.path.join(os.path.dirname(__file__) or ".", "series-info-scraped-from-docs.csv")
_series_info_path = os.path.join(os.path.dirname(__file__) or ".", "all-series.csv")

if os.path.isfile(_series_info_path):
    _df_series = pd.read_csv(_series_info_path, usecols=["SKU", "Memory (GB)", "Processor", "Memory (GiB)","Memory: GiB", "vCPUs (Qty.)", "Category", "File","Network bandwidth","Max network bandwidth (Mbps)","Max Network Bandwidth (Mbps)","Max Network Bandwidth (Mb/s)","RAM (GiB)","Cores (Qty.)","Live Migration","Nested Virtualization"])
    _df_series["SKU"] = _df_series["SKU"].str.strip()
    _mem = _df_series.dropna(subset=["SKU", "Memory (GB)"])
    _MEMORY_LOOKUP = dict(zip(_mem["SKU"], _mem["Memory (GB)"]))

    _mem2 = _df_series.dropna(subset=["SKU", "Memory (GiB)"])
    _MEMORY_LOOKUP2 = dict(zip(_mem2["SKU"], _mem2["Memory (GiB)"]))

    _mem3 = _df_series.dropna(subset=["SKU", "Memory: GiB"])
    _MEMORY_LOOKUP3 = dict(zip(_mem3["SKU"], _mem3["Memory: GiB"]))

    _mem4 = _df_series.dropna(subset=["SKU", "RAM (GiB)"])
    _MEMORY_LOOKUP4 = dict(zip(_mem4["SKU"], _mem4["RAM (GiB)"]))

    _category = _df_series.dropna(subset=["SKU", "Category"])
    _CATEGORY_LOOKUP = dict(zip(_category["SKU"], _category["Category"]))

    _doc_path = _df_series.dropna(subset=["SKU", "File"])
    _DOCUMENT_PATH_LOOKUP = dict(zip(_doc_path["SKU"], _doc_path["File"]))

    _networkbw = _df_series.dropna(subset=["SKU", "Network bandwidth"])
    _NETWORKBW_LOOKUP = dict(zip(_networkbw["SKU"], _networkbw["Network bandwidth"]))

    _networkbw2 = _df_series.dropna(subset=["SKU", "Max network bandwidth (Mbps)"])
    _NETWORKBW2_LOOKUP = dict(zip(_networkbw2["SKU"], _networkbw2["Max network bandwidth (Mbps)"]))

    _networkbw3 = _df_series.dropna(subset=["SKU", "Max Network Bandwidth (Mbps)"])
    _NETWORKBW3_LOOKUP = dict(zip(_networkbw3["SKU"], _networkbw3["Max Network Bandwidth (Mbps)"]))

    _networkbw4 = _df_series.dropna(subset=["SKU", "Max Network Bandwidth (Mb/s)"])
    _NETWORKBW4_LOOKUP = dict(zip(_networkbw4["SKU"], _networkbw4["Max Network Bandwidth (Mb/s)"]))

    _proc = _df_series.dropna(subset=["SKU", "Processor"])
    _PROCESSOR_LOOKUP = dict(zip(_proc["SKU"], _proc["Processor"].str.strip()))

    _vcpu = _df_series.dropna(subset=["SKU", "vCPUs (Qty.)"])
    _VCPU_LOOKUP = dict(zip(_vcpu["SKU"], pd.to_numeric(_vcpu["vCPUs (Qty.)"], errors="coerce")))

    _vcpu2 = _df_series.dropna(subset=["SKU", "Cores (Qty.)"])
    _VCPU_LOOKUP2 = dict(zip(_vcpu2["SKU"], pd.to_numeric(_vcpu2["Cores (Qty.)"], errors="coerce")))

    _live_migration = _df_series.dropna(subset=["SKU", "Live Migration"])
    _LIVE_MIGRATION_LOOKUP = dict(zip(_live_migration["SKU"], _live_migration["Live Migration"]))

    _nested_virtualization = _df_series.dropna(subset=["SKU", "Nested Virtualization"])
    _NESTED_VIRTUALIZATION_LOOKUP = dict(zip(_nested_virtualization["SKU"], _nested_virtualization["Nested Virtualization"]))

    del _df_series, _mem, _mem2,  _proc, _vcpu, _vcpu2, _category, _doc_path, _networkbw, _networkbw2, _networkbw3, _networkbw4, _live_migration, _nested_virtualization

def lookup_memory(sku: str) -> int:
    """Lookup memory for a SKU from the series-info CSV data."""
    mem = _MEMORY_LOOKUP.get(sku)
    if mem is None:
        mem = _MEMORY_LOOKUP2.get(sku)
        if mem is None:
            mem = _MEMORY_LOOKUP3.get(sku)
            if mem is None:
                mem = _MEMORY_LOOKUP4.get(sku)
                if mem is None:
                    return None
            
    mem = float(mem.replace(",", "")) if isinstance(mem, str) else mem
    return mem

def lookup_networkbw(sku: str) -> int:
    """Lookup network bandwidth for a SKU from the series-info CSV data."""
    bw = _NETWORKBW_LOOKUP.get(sku)
    if bw is not None:
        bw = str(bw).replace(",", "").replace(".0", "").replace("+", "")  # Clean up formatting
        return int(bw)
    bw2 = _NETWORKBW2_LOOKUP.get(sku)
    if bw2 is not None:
        bw2 = str(bw2).replace(",", "").replace(".0", "").replace("+", "")
        return int(bw2)
    bw3 = _NETWORKBW3_LOOKUP.get(sku)
    if bw3 is not None:
        bw3 = str(bw3).replace(",", "").replace(".0", "").replace("+", "")
        return int(bw3)
    bw4 = _NETWORKBW4_LOOKUP.get(sku)
    if bw4 is not None:
        bw4 = str(bw4).replace(",", "").replace(".0", "").replace("+", "")
        return int(bw4)
    return ""

def lookup_vcpu(sku: str) -> int:
    """Lookup vCPU count for a SKU from the series-info CSV data."""
    vcpu = _VCPU_LOOKUP.get(sku)
    if vcpu is not None:
        return vcpu
    vcpu2 = _VCPU_LOOKUP2.get(sku)
    if vcpu2 is not None:
        return vcpu2
    return None

def parse_sku(sku: str, product_name: str, meter: str) -> dict:
    """
    Parse an Azure VM SKU name and return enriched metadata.

    Extracts vCPU count, estimated memory, version, feature flags, meter type,
    OS type, CPU vendor, and retirement/burstable notes from the SKU string,
    product name, and meter name.

    Returns dict with keys: vCPUs, Memory_GB, Memory_Ratio, Version, Features, MeterType,
    OSType, Notes, CPUType.
    """
    result = {"vCPUs": "", "Memory_GB": "", "Version": "", "Features": "", "MeterType": "", "OSType": "", "Notes": "", "CPUType": "", "Memory_Ratio": "", "Category": "", "DocumentPath": "", "NetworkBandwidth": ""}

    # ---------------------------------------------------------------------------
    # Memory-per-core ratio (GB) by VM family letter.
    # These are approximate base ratios used to estimate total memory from the
    # vCPU count.  Constrained-core and modifier-letter variants (m/l/t/x) are
    # handled with multipliers further below.
    # ---------------------------------------------------------------------------
    FAMILY_MEM_RATIO: dict[str, int] = {
        "A": 2,    # General purpose (legacy)
        "B": 4,    # Burstable
        "D": 4,    # General purpose
        "E": 8,    # Memory optimised
        "F": 2,    # Compute optimised
        "G": 8,    # Storage / memory (legacy)
        "H": 8,    # HPC
        "L": 8,    # Storage optimised
        "M": 16,   # Very-large memory
        "N": 4,    # GPU – memory varies widely; rough default
        "P": 4,    # ARM-based (Ampere)
    }

    # Legacy A-series VMs have fixed (non-formula) core/memory mappings
    LEGACY_A_SIZES: dict[str, tuple[int, float]] = {
        "A0": (1, 0.75), "A1": (1, 1.75), "A2": (2, 3.5), "A3": (4, 7),
        "A4": (8, 14),   "A5": (2, 14),    "A6": (4, 28),  "A7": (8, 56),
        "A8": (8, 56),   "A9": (16, 112),  "A10": (8, 56), "A11": (16, 112),
    }

    # Feature-letter descriptions – each lowercase letter after the core count
    # in a modern SKU name indicates a hardware or capability trait.
    FEATURE_MAP: dict[str, str] = {
        "a": "AMD processor",
        "b": "Block storage performance",
        "c": "Confidential",
        "d": "Local temp disk",
        "i": "Isolated size",
        "l": "Low memory",
        "m": "Memory intensive",
        "p": "ARM (Ampere) processor",
        "s": "Premium storage capable",
        "t": "Tiny memory",
        "x": "Extra-large memory",
        "n": "Network Optimized",
    }

    # For these machine SKU return static results

    if "Type" in sku or sku == "ARM_SKU_NAME_PLACEHOLDER":
        result["Notes"] = "Dedicated Host"
        result["Category"] = "Not Documented"
        return result

    if "Standard_G" in sku and not ("_v" in sku):
        result["Notes"] = "Retired-Avoid-Use"
        result["Category"] = "Retired"
        return result


    # --- Determine the SKU version suffix (e.g. "v5", "v2_Promo") ---
    if (sku.startswith("NC")):
        version = sku[-2:]  # NC-series encodes version in last 2 chars
    elif(sku.endswith("Promo") and sku.find("_v")>0):
        version = sku[-8:]  # Promotional SKUs keep the full "_vN_Promo" suffix
    elif(sku.endswith("_v32")):
        version = ""        # Special case – not a real version tag
    elif(sku.find("_v") > 0):
        version = sku[-2:]  # Standard version suffix (e.g. "v5")
    else:
        version = ""        # No version suffix present
    result["Version"] = f"{version}"


    # Source Memory and CPU cores and Type from docs-scraped CSV lookup if available
    
    constrained_cores = None
    
    if "_Promo" in sku:
            base_sku = sku.replace("_Promo", "")
    elif "-" in sku:
            # This indicates the SKU has constraint information embedded (e.g. E64-16s)
            base_sku = re.sub(r'-(\d+)(?=[^_]*_)', "", sku) # Remove constrained core count for lookup (e.g. E64-16s → E64s)
            match = re.search(r'-(\d+)', sku)
            constrained_cores = int(match.group(1).replace("-", ""))
    else:
            base_sku = sku
    
    #if it is a constrained SKU, set cores to constrained core count, otherwise lookup cores from the CSV data or parse from SKU name
    if constrained_cores:
        cores = constrained_cores
    else:
        cores = lookup_vcpu(base_sku)
    
    # However, if we could not find it in the database
    # Figure it out based on the SKU
    if cores is None:
        cores = int(re.search(r"(\d+)", sku).group(1))

    result["vCPUs"] = cores

    processor_str = _PROCESSOR_LOOKUP.get(base_sku)
    if processor_str is not None:
        result["CPUType"] = processor_str
        
        # calculate CPU vendor
        if "Intel" in processor_str:
            result["CPUVendor"] = "Intel"
        elif "AMD" in processor_str:
            result["CPUVendor"] = "AMD"
        elif "Ampere" in processor_str:
            result["CPUVendor"] = "Ampere"
        elif "Azure Cobalt" in processor_str:
            result["CPUVendor"] = "Azure"
        elif "NVIDIA" in processor_str or "Nvidia" in processor_str:
            result["CPUVendor"] = "Nvidia"
        else:
            result["CPUVendor"] = "Other"    

   

    # Memory lookup
    memory_gb = lookup_memory(base_sku)

        #print (f"Parsed SKU: {sku} → vCPUs: {result['vCPUs']}, Memory_GB: {memory_gb}, CPUType: {result['CPUType']}, Features: {result['Features']}")
    try:
        if memory_gb is not None :
            result["Memory_GB"] = memory_gb
            if cores is not None and cores > 0:
                result["Memory_Ratio"] = f"1 core to {round(int(memory_gb) / int(cores),2)} GB RAM" if cores else ""
    except Exception as e:
        print(f"Error {sku} {base_sku}: {e}")    

    # category
    category = _CATEGORY_LOOKUP.get(base_sku)
    if category is not None:
        result["Category"] = category
    else:
        result["Category"] = "Not Documented"
    
    # document path
    doc_path = _DOCUMENT_PATH_LOOKUP.get(base_sku)
    if doc_path is not None:
        result["DocumentPath"] = f"https://learn.microsoft.com/en-us/azure{doc_path}"

    # network bandwidth
    network_bw = lookup_networkbw(base_sku)
    if network_bw:
        result["NetworkBandwidth"] = network_bw


    # Strip tier prefix (Standard_ or Basic_) to simplify regex matching
    name = base_sku

    for prefix in ("Standard_", "Basic_"):
        if name.startswith(prefix):
            name = name[len(prefix):]
            break

    # ---- Special / accelerator SKUs (NV*ads_V710, NC*_RTXPRO, PB*) ----
    # Attempt to extract cores/features even from non-standard naming.

    # Main regex for modern Azure VM SKU names.
    # Captures: Family, vCPUs, optional constrained cores, feature letters,
    # optional accelerator suffix, and optional version number.
    m = re.match(
        r"^([A-Z]{1,2})"           # 1: family (D, E, NC, NV, EC, HB …)
        r"(\d+)"                    # 2: vCPU count
        r"(?:-(\d+))?"             # 3: constrained vCPU count (optional)
        r"([a-z]*)"                # 4: feature letters (ads, ls, ms …)
        r"(?:_([a-zA-Z0-9]+))?"   # 5: accelerator or sub-variant (V710, A10, xl …)
        r"(?:_v?(\d+))?$",        # 6: version (v2, v5 …)
        name,
    )

    if not m:
        # Fallback: if the regex didn't match, try to extract at least a core count
        nums = re.findall(r"\d+", name)
        if nums:
            result["Version"] = f"{version}"
            #result["vCPUs"] = int(nums[0])
    else:
        # Successful parse – extract all captured groups
        family = m.group(1)
        #cores = int(m.group(2))
        #constrained = int(m.group(3)) if m.group(3) else None  # e.g. E64-16s → 16 usable vCPUs
        feat_letters = m.group(4) or ""
        accel = m.group(5) or ""
        result["Version"] = f"{version}"


        result["Family"] = family
        # Use constrained core count if present; otherwise use advertised cores
        # result["vCPUs"] = cores

 
        # ---- Features ----
        # Translate each feature letter into a human-readable description.
        features: list[str] = []

        for ch in feat_letters:
            desc = FEATURE_MAP.get(ch)
            if desc and desc not in features:
                features.append(desc)
        
        if "C" in family:
            features.append("Confidential Computing")
            result["Category"] += ";Confidential Computing"

        if constrained_cores:
            features.append("Constrained Cores")

        # Live Migration
        live_migration = _LIVE_MIGRATION_LOOKUP.get(base_sku)
        if live_migration is not None:
            if "Supported" == live_migration or "Restricted Support" == live_migration:
                features.append("Live Migration Supported")


        # Nested Virtualization
        nested_virtualization = _NESTED_VIRTUALIZATION_LOOKUP.get(base_sku)
        if nested_virtualization is not None:
            if "Supported" == nested_virtualization:
                features.append("Nested Virtualization Supported")


        # Fallback in case the SKU was not found in the series-info CSV
        if not processor_str:
            if "a" in feat_letters:
                result["CPUType"] = "AMD"
            elif "p" in feat_letters:
                result["CPUType"] = "ARM"
            elif "_HB" in sku or "_HX" in sku or "_NM" in sku:
                result["CPUType"] = "AMD"
            #else:
                # Assuming it is Intel if not AMD or ARM, but this may not always be correct
                # result["CPUType"] = "Intel"


        # ---- Accelerator info ----
        # Identify specific GPU/accelerator hardware from the suffix.
        if accel:
            # Known GPU accelerators
            if "A100" in accel.upper():
                features.append("NVIDIA A100 GPU")            
            elif "V710" in accel:
                features.append("AMD Radeon V710 GPU")
            elif "RTX" in accel:
                features.append("NVIDIA RTX PRO GPU")
            elif "H100" in accel.upper():
                features.append("NVIDIA H100 GPU")
            elif "H200" in accel.upper():
                features.append("NVIDIA H200 GPU")
            elif "A10" in accel:
                features.append("NVIDIA A10 GPU")
            # elif accel.lower() not in ("v1", "v2", "v3", "v4", "v5", "v6","v7"):
            #     features.append(f"Accelerator: {accel}")


        # Calculate if a SKU is the base model
        if sku[-3:] in ("_v5", "_v4", "_v3"):
            if feat_letters in ("", "a","ps"):
                features.append("Base model (v3-v7 only)")
        if sku[-3:] in ("_v7", "_v6"):
            if feat_letters in ("", "as", "s", "ps","isr"):
                features.append("Base model (v3-v7 only)")

        result["Features"] = "; ".join(features)


    # ---- Meter Type ----
    # Classify the pricing tier from the meter name.
    if meter:
        if "Low Priority" in meter:
            result["MeterType"] = "Low Priority"
        elif "Spot" in meter:
            result["MeterType"] = "Spot"
        else:
            result["MeterType"] = "On Demand"

    # ---- OS Type ----
    # Derive the OS from the product name string.
    if product_name:
        if "Windows" in product_name:
            result["OSType"] = "Windows"
        elif "Linux" in product_name:
            result["OSType"] = "Linux"
        else:
            result["OSType"] = "Unspecified"

    # ---- Notes ----
    # Flag SKUs that are being retired or have special characteristics.
    notes: list[str] = []

    # Mark older generations and specific series scheduled for retirement
    if sku.endswith("_v2") or sku.endswith("_v2_Promo"):
        notes.append("Retiring-Avoid-Use")
    if sku.startswith("Standard_NP") or sku.startswith("Standard_HC"):
        notes.append("Retiring-Avoid-Use")
    if sku.startswith("NC") and sku.endswith("_v3") and not sku.endswith("T4_v3"):
        notes.append("Retiring-Avoid-Use")

    if sku.startswith("Standard_DS") and not ("_v" in sku):
        notes.append("Retiring-Avoid-Use")
    if sku.startswith("Standard_F") and not ("_v" in sku):
        notes.append("Retiring-Avoid-Use")
    if sku.startswith("Standard_G") and not ("_v" in sku):
        notes.append("Retiring-Avoid-Use")
    if sku.startswith("Standard_L") and not ("_v" in sku):
        notes.append("Retiring-Avoid-Use")


    if sku.startswith("Standard_G5") or sku.startswith("Standard_GS5"):
        notes.append("Retiring-Avoid-Use")
    if sku.startswith("Standard_E64i_v3") or sku.startswith("Standard_E64is_v3"):
        notes.append("Retiring-Avoid-Use")
    if sku.startswith("Standard_M192is_v2") or sku.startswith("Standard_M192ims_v2") or sku.startswith("Standard_M192ids_v2") or sku.startswith("Standard_M192idms_v2"):
        notes.append("Retiring-Avoid-Use")

    # Flag burstable B-series VMs (credit-based CPU model)
    if sku.startswith("Standard_B"):
        notes.append("Burstable")

    result["Notes"] = "; ".join(notes)


    # ---- Dedicated-host / type SKUs (e.g. "Dadsv5_Type1") – skip parsing ----
    
    if "Type" in sku or sku == "ARM_SKU_NAME_PLACEHOLDER":
        result["Notes"] = "Dedicated Host"

    # ---- Legacy A-series (A0-A11) without version suffix ----
    if re.match(r"^A\d+$", name):
        if name in LEGACY_A_SIZES:
            cores, mem = LEGACY_A_SIZES[name]
            result["vCPUs"] = cores
            result["Memory_GB"] = mem
            result["Notes"] = "Legacy A-series"

    # Everything else
    result["Version"] = f"{version}"

    # The current date in format "M/D/YYYY"
    result["Date"] = datetime.now().strftime("%m/%d/%Y")

    return result

def build_pricing_table(json_data, table_data):
    """Append rows from a single API response page to the pricing table list."""
    for item in json_data['Items']:
        meter = item['meterName']
        table_data.append([item['armSkuName'], item['retailPrice'],  item['armRegionName'], meter, item['productName'], item['unitOfMeasure']])
        
def requests_get_with_retry(url, max_retries=5, initial_delay=1, **kwargs):
    """Perform a GET request with exponential backoff on failure."""
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            response = requests.get(url, **kwargs)
            response.raise_for_status()
            return response
        except (requests.exceptions.RequestException) as e:
            if attempt == max_retries - 1:
                raise
            print(f"  Request failed ({e}), retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2

def pull_data_from_api(regions):
    """
    Query the Azure Retail Prices API for VM pricing in each target region.
    Handles pagination automatically and writes one CSV per region.
    """

    # Target regions to fetch pricing for


    for region in regions:

        if len(region) < 6:
            print(f"Skipping invalid region name: '{region}'")
            print("Make sure there are at least 2 regions specified.")
            print(f"Current regions array: {regions}")
            continue

        # For every region, build the OData filter and make the initial request
        print ("Downloading price data for region",region)

        # Initialize table with header row
        table_data = []
        table_data.append(['SKU', 'Retail Price', 'Region', 'Meter', 'Product Name','unitOfMeasure'])
        
        # Azure Retail Prices REST endpoint
        api_url = "https://prices.azure.com/api/retail/prices?api-version=2021-10-01-preview"
        
        # OData filter: restrict to VM service in the target region
        query = "armRegionName eq '" + region +"' and serviceName eq 'Virtual Machines'" + " and priceType eq 'Consumption'"
        response = requests_get_with_retry(api_url, params={'$filter': query})
        json_data = json.loads(response.text)
        
        build_pricing_table(json_data, table_data)
        nextPage = json_data['NextPageLink']
        
        # Follow pagination links until all pages are consumed
        while(nextPage):
            time.sleep(1) # Sleep 1 second between requests to avoid hitting rate limits
            response = requests_get_with_retry(nextPage)
            json_data = json.loads(response.text)
            nextPage = json_data['NextPageLink']
            build_pricing_table(json_data, table_data)
            print("  Getting next page...")

        # Write the collected pricing data to a CSV file for this region
        fileName = region + '_pricing_data.csv'

        with open(fileName, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            #writer.writerow("armSkuName,retailPrice,unitOfMeasure,armRegionName,meter,productName")
            writer.writerows(table_data)
        print ("Wrote",fileName)

def merge_data():
    """
    Read all per-region pricing CSVs, concatenate them, and create a pivoted
    "combined.csv" with one row per (SKU, Meter, Product Name) and one column
    per region containing the retail price.
    """
    # Find all CSV files with "pricing_data" in the name (one per region)
    csv_files = glob.glob(os.path.join(os.path.dirname(__file__) or ".", "*pricing_data*.csv"))

    if not csv_files:
        print("No CSV files with 'pricing_data' in the name found.")
        exit(1)

    print(f"Found {len(csv_files)} file(s): {[os.path.basename(f) for f in csv_files]}")

    # Load all regional files and merge into a single DataFrame
    frames = [pd.read_csv(f) for f in csv_files]
    df = pd.concat(frames, ignore_index=True)

    # Normalize column names (strip leading/trailing whitespace)
    df.columns = df.columns.str.strip()

    # Pivot: one row per unique (SKU, Meter, Product Name) combination,
    # one column per Region containing the retail price.
    # Where duplicates exist in the same region, keep the first occurrence.
    pivoted = df.pivot_table(
        index=["SKU",  "Meter", "Product Name"],
        columns="Region",
        values="Retail Price",
        aggfunc="first",
    )

    # Flatten the multi-level column index and reset
    pivoted.columns = [col for col in pivoted.columns]
    pivoted.reset_index(inplace=True)

    output = os.path.join(os.path.dirname(__file__) or ".", "combined.csv")
    pivoted.to_csv(output, index=False)
    print(f"Written {len(pivoted)} rows to {output}")


def add_sku_details():
    """
    Read combined.csv, parse each SKU to extract hardware metadata, apply
    exclusion filters, and write the final enriched report to
    "sku-price-report-by-region.csv".
    """
    script_dir = os.path.dirname(__file__) or "."
    input_path = os.path.join(script_dir, "combined.csv")
    output_path = os.path.join(script_dir, "sku-price-report-by-region.csv")

    with open(input_path, newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        fieldnames = list(reader.fieldnames or [])

        # Insert enrichment columns immediately after the SKU column
        sku_idx = fieldnames.index("SKU") + 1
        new_cols = ["Date","Family","vCPUs", "Memory_GB", "Memory_Ratio", "Version", "MeterType", "OSType","CPUVendor","CPUType","Notes", "Features", "Category", "DocumentPath", "NetworkBandwidth"]
        for i, col in enumerate(new_cols):
            fieldnames.insert(sku_idx + i, col)

        # Parse each row and apply exclusion filters
        rows: list[dict] = []
        for row in reader:
            details = parse_sku(row.get("SKU", ""), row.get("Product Name", ""),row.get("Meter", ""))
            row.update(details)

            # Apply exclusion filters based on configuration flags
            if exclude_basic_or_legacy and ("Legacy A-series" in row.get("Notes", "") or row.get("SKU", "").startswith("Standard_A")):
                continue
            if exclude_retiring and "Retiring-Avoid-Use" in row.get("Notes", ""):
                continue
            if exclude_dedicated_host and "Dedicated Host" in row.get("Notes", ""):
                continue
            if exclude_dedicated_host and "Isolated" in row.get("Features", ""):
                continue
            if exclude_retired and "Retired" in row.get("Category", ""):
                continue
            if exclude_not_documented and "Not Documented" in row.get("Category", ""):
                continue
            if exclude_spot and (row.get("MeterType", "") == "Spot"):
                continue
            if exclude_low_priority and row.get("MeterType", "") == "Low Priority":
                continue
            if exclude_on_demand and row.get("MeterType", "") == "On Demand":
                continue
            if exclude_cloud_services:
                if "CloudServices" in row.get("Product Name", ""):
                    continue
                elif "Cloud Services" in row.get("Product Name", ""):
                    continue

            rows.append(row)

    if export_report_to_json:
        json_output_path = os.path.join(script_dir, "../data/sku-price-report-by-region.json")
        with open(json_output_path, "w", encoding="utf-8") as json_out:
            json.dump(rows, json_out, indent=2)
        print(f"Written {len(rows)} rows to {json_output_path}")
    
    if export_report_to_csv:
        with open(output_path, "w", newline="", encoding="utf-8") as fout:
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print(f"Written {len(rows)} rows to {output_path}")


if __name__ == "__main__":

    # --- Exclusion filters ---
    # Set to True to omit the corresponding category from the final report.
    exclude_basic_or_legacy  = True   # Filter legacy A-series / Basic tier SKUs
    exclude_retiring         = False   # Filter SKUs flagged as retiring
    exclude_retired          = True   # Filter SKUs flagged as retired (e.g. Standard_G1-G4)
    exclude_dedicated_host   = True   # Filter dedicated and isolated host type SKUs
    exclude_not_documented   = True   # Filter SKUs flagged as not documented
    exclude_low_priority     = False  # Filter Low Priority pricing - v6/v7 no longer uses this category
    exclude_cloud_services   = True  # Filter cloud service SKUs (e.g. A8-A11) that share names with VM SKUs but have different pricing

    exclude_spot             = False  # Filter Spot
    exclude_on_demand        = False  # Filter On Demand pricing

    export_report_to_csv     = False
    export_report_to_json    = True

    regions = ("eastus2","eastus","centralus","westus3","northcentralus","southcentralus")

    # --- Execute the three-step pipeline ---
    pull_data_from_api(regions)   # Step 1: Fetch per-region pricing from the API
    merge_data()                   # Step 2: Combine regional CSVs into pivoted table
    add_sku_details()              # Step 3: Enrich with parsed SKU metadata & export

    

