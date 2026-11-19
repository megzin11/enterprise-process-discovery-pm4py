# enterprise_p2p_pipeline.py
import os
import pandas as pd
import pm4py

graphviz_path = r'C:\Program Files\Graphviz\bin'
os.environ["PATH"] += os.pathsep + graphviz_path

def setup_directories():
    """Create necessary directories for the repository."""
    for folder in ["data", "visualizations"]:
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"[+] Created directory: {folder}/")


def run_etl_pipeline(input_path, output_path):
    print(f"\n[*] STEP 1: Ingesting & Transforming ERP extract from {input_path}")
    
    # Some massive enterprise CSVs use different encodings or separators. 
    # If it fails to read, try adding sep=';' or encoding='utf-8-sig'
    df = pd.read_csv(input_path)
    
    # 1. Diagnostic: Print the exact columns found in the CSV
    print(f"[*] Raw CSV Columns Detected: {df.columns.tolist()}")
    
    # 2. Resilient Column Mapping
    # This dictionary catches common column name variations and maps them to PM4Py standards
    col_mapping = {
        # Case ID variations
        'Order_ID': 'case:concept:name',
        'case': 'case:concept:name',
        'case concept:name': 'case:concept:name',
        'Case ID': 'case:concept:name',
        
        # Activity variations
        'Action_Step': 'concept:name',
        'event': 'concept:name',
        'event concept:name': 'concept:name',
        'Activity': 'concept:name',
        
        # Timestamp variations
        'Timestamp': 'time:timestamp',
        'startTime': 'time:timestamp',
        'event time:timestamp': 'time:timestamp',
        'Complete Timestamp': 'time:timestamp'
    }
    
    # Apply only the mappings that actually match the current dataframe
    df.rename(columns=col_mapping, inplace=True)
    
    # 3. Architectural Safety Check
    if 'case:concept:name' not in df.columns or 'time:timestamp' not in df.columns:
        print("\n[-] FATAL ETL ERROR: Core mapping failed.")
        print("[-] Look at the 'Raw CSV Columns' printed above. Find the actual names for Case ID and Timestamp, and add them to the 'col_mapping' dictionary in the code.")
        return # Stops the script cleanly instead of throwing a KeyError
        
    # 4. Standardize Timestamps
    # Use format='mixed' to handle messy enterprise timezone data
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'], format='mixed', utc=True)
    
    # 5. Sort and Format
    df = df.sort_values(by=['case:concept:name', 'time:timestamp'])
    
    print("[*] Formatting to PM4Py Event Log standard...")
    event_log = pm4py.format_dataframe(df, case_id='case:concept:name', 
                                       activity_key='concept:name', timestamp_key='time:timestamp')
    
    pm4py.write_xes(event_log, output_path)
    print(f"[+] ETL Complete. Standardized XES log saved to: {output_path}")

def discover_process_model(log_path, output_image_path):
    """Discovers the operational Petri net from the event log."""
    print(f"\n[*] STEP 2: Loading XES for Petri Net Discovery...")
    log = pm4py.read_xes(log_path)
    #VARIANT FILTERING
    print("[*] Filtering out chaotic outliers (keeping the top 10 most common pathways)...")
    # This keeps the main business flows and drops the rare 'spaghetti' edge cases
    log = pm4py.filter_variants_top_k(log, k=10)
    print("[*] Applying Inductive Miner algorithm...")
    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log)
    
    pm4py.save_vis_petri_net(net, initial_marking, final_marking, output_image_path)
    print(f"[+] Petri Net visualization saved to: {output_image_path}")

def analyze_bottlenecks(log_path, output_image_path):
    """Calculates throughput friction and end-to-end cycle times."""
    print(f"\n[*] STEP 3: Executing Performance & Bottleneck Analysis...")
    log = pm4py.read_xes(log_path)
    
    perf_dfg, start_activities, end_activities = pm4py.discover_performance_dfg(log)
    pm4py.save_vis_performance_dfg(perf_dfg, start_activities, end_activities, output_image_path)
    print(f"[+] Bottleneck Heatmap saved to: {output_image_path}")
    
    all_case_durations = pm4py.get_all_case_durations(log)
    if all_case_durations:
        avg_duration = sum(all_case_durations) / len(all_case_durations)
        print(f"[+] OVERALL METRIC: Average End-to-End P2P Cycle Time: {avg_duration / 86400:.2f} Days")

if __name__ == "__main__":
    # 1. Force absolute paths 
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    setup_directories()
    
    # 2. Point directly to the REAL BPIC XES Dataset
    REAL_XES_DATA = os.path.join(BASE_DIR, "data", "BPI_Challenge_2019.xes")
    PETRI_NET_IMG = os.path.join(BASE_DIR, "visualizations", "bpic_petri_net.png")
    BOTTLENECK_IMG = os.path.join(BASE_DIR, "visualizations", "bpic_throughput.png")
    
    # 3. Execute Discovery Pipeline (Skipping ETL because it's already an XES file!)
    print(f"\n[!] Initiating AI Discovery on Real Enterprise Data: {REAL_XES_DATA}")
    
    # Make sure your BPI_Challenge_2019.xes file is actually inside the data/ folder!
    if os.path.exists(REAL_XES_DATA):
        discover_process_model(REAL_XES_DATA, PETRI_NET_IMG)
        analyze_bottlenecks(REAL_XES_DATA, BOTTLENECK_IMG)
        print("\n[✓] Enterprise Process Discovery Pipeline executed successfully.")
    else:
        print(f"\n[-] ERROR: Could not find the file at {REAL_XES_DATA}")
        print("[-] Please move BPI_Challenge_2019.xes into your 'data' folder.")