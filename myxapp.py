import src.e2ap_xapp as e2ap_xapp
#from ran_messages_pb2 import *
from time import sleep
from ricxappframe.e2ap.asn1 import IndicationMsg

import sys
import os
import time
import argparse
sys.path.append("oai-oran-protolib/builds/")
from ran_messages_pb2 import *


def check_csv_file_status(csv_filename):
    """Check and log the status of the CSV file"""
    try:
        print(f"[XAPP_CSV_CHECK] Checking status of CSV file: {csv_filename}")
        if os.path.exists(csv_filename):
            file_size = os.path.getsize(csv_filename)
            mod_time = os.path.getmtime(csv_filename)
            mod_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mod_time))
            print(f"[XAPP_CSV_CHECK] File exists: Size={file_size} bytes, Last modified: {mod_time_str}")
            
            # Read and print first few lines of the file
            try:
                with open(csv_filename, 'r') as f:
                    lines = f.readlines()[:5]  # Read first 5 lines
                    print(f"[XAPP_CSV_CHECK] File content preview (first {len(lines)} lines):")
                    for i, line in enumerate(lines):
                        print(f"[XAPP_CSV_CHECK] Line {i+1}: {line.strip()}")
            except Exception as e:
                print(f"[XAPP_CSV_CHECK] Error reading file content: {str(e)}")
        else:
            print(f"[XAPP_CSV_CHECK] WARNING: File does not exist: {csv_filename}")
    except Exception as e:
        print(f"[XAPP_CSV_CHECK] ERROR checking file status: {str(e)}")



def xappLogic(output_dir=None):
    import csv
    import os
    from datetime import datetime

    # instanciate xapp 
    connector = e2ap_xapp.e2apXapp()

    # get gnbs connected to RIC
    gnb_id_list = connector.get_gnb_id_list()
    print("[XAPP_LOG] {} gNB connected to RIC, listing:".format(len(gnb_id_list)))
    for gnb_id in gnb_id_list:
        print("[XAPP_LOG] gNB ID: {}".format(gnb_id))
    print("[XAPP_LOG] ---------")

    # Create CSV file for data collection
    if output_dir:
        # Ensure output directory exists
        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
                print(f"[XAPP_LOG] Created output directory: {output_dir}")
            except Exception as e:
                print(f"[XAPP_LOG] ERROR: Failed to create output directory {output_dir}: {str(e)}")
                print(f"[XAPP_LOG] Falling back to current directory")
                output_dir = None
    
    # Set CSV filename with path
    if output_dir:
        csv_filename = os.path.join(output_dir, "phy_mac_metrics.csv")
    else:
        csv_filename = "phy_mac_metrics.csv"
    
    csv_file_exists = os.path.isfile(csv_filename)
    
    # Print current working directory and absolute path of CSV file
    current_dir = os.getcwd()
    csv_abs_path = os.path.abspath(csv_filename)
    print(f"[XAPP_LOG] Current working directory: {current_dir}")
    print(f"[XAPP_LOG] CSV file absolute path: {csv_abs_path}")
    
    with open(csv_filename, 'a', newline='') as csvfile:
        fieldnames = ['timestamp', 'gnb_id', 'rnti', 'rsrp', 'ber_ul', 'ber_dl', 'mcs_ul', 'mcs_dl', 'cell_load']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not csv_file_exists:
            writer.writeheader()
            print(f"[XAPP_LOG] Created new CSV file: {csv_filename}")
        else:
            print(f"[XAPP_LOG] Appending to existing CSV file: {csv_filename}")

    # subscription requests
    for gnb in gnb_id_list:
        e2sm_buffer = e2sm_report_request_buffer()
        connector.send_e2ap_sub_request(e2sm_buffer, gnb)
        print(f"[XAPP_LOG] Sent subscription request to gNB {gnb}")
    
    # read loop with 500ms interval
    sleep_time = 0.5  # 500ms
    print(f"[XAPP_LOG] Starting data collection loop with {sleep_time*1000}ms interval")
    
    loop_count = 0
    while True:
        loop_count += 1
        sleep(sleep_time)
        messgs = connector.get_queued_rx_message()
        
        # Periodically check CSV file status (every 20 loops = ~10 seconds)
        if loop_count % 20 == 0:
            check_csv_file_status(csv_filename)
        
        if len(messgs) > 0:
            print(f"[XAPP_LOG] {len(messgs)} messages received")
            
            for msg in messgs:
                if msg["message type"] == connector.RIC_IND_RMR_ID:
                    gnb_id = msg["meid"]
                    print(f"[XAPP_LOG] RIC Indication from gNB {gnb_id}")
                    
                    indm = IndicationMsg()
                    indm.decode(msg["payload"])
                    resp = RAN_indication_response()
                    resp.ParseFromString(indm.indication_message)
                    
                    # Extract data and save to CSV
                    timestamp = None
                    cell_load = None
                    ue_list = None
                    
                    for param in resp.param_map:
                        if param.key == RAN_parameter.TIMESTAMP:
                            timestamp = param.timestamp
                        elif param.key == RAN_parameter.CELL_LOAD:
                            cell_load = param.cell_load
                        elif param.key == RAN_parameter.UE_LIST:
                            ue_list = param.ue_list
                    
                    if timestamp and ue_list:
                        # Format timestamp for display
                        timestamp_str = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        print(f"[XAPP_LOG] Timestamp: {timestamp_str}, Cell Load: {cell_load}%")
                        
                        # Save data to CSV
                        try:
                            with open(csv_filename, 'a', newline='') as csvfile:
                                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                                ue_count = 0
                                
                                for ue in ue_list.ue_info:
                                    row = {
                                        'timestamp': timestamp,
                                        'gnb_id': gnb_id,
                                        'rnti': ue.rnti,
                                        'rsrp': ue.rsrp,
                                        'ber_ul': ue.ber_ul,
                                        'ber_dl': ue.ber_dl,
                                        'mcs_ul': ue.mcs_ul,
                                        'mcs_dl': ue.mcs_dl,
                                        'cell_load': cell_load
                                    }
                                    writer.writerow(row)
                                    ue_count += 1
                                    print(f"[XAPP_LOG] UE {ue.rnti}: RSRP={ue.rsrp}dBm, BER UL={ue.ber_ul}, BER DL={ue.ber_dl}, MCS UL={ue.mcs_ul}, MCS DL={ue.mcs_dl}")
                                
                                # Check file size after writing
                                file_size = os.path.getsize(csv_filename)
                                print(f"[XAPP_LOG] CSV FILE STATUS: Wrote data for {ue_count} UEs, current file size: {file_size} bytes")
                        except Exception as e:
                            print(f"[XAPP_LOG] ERROR: Failed to write to CSV file: {str(e)}")
                        else:
                            print(f"[XAPP_LOG] SUCCESS: Data successfully saved to CSV file {csv_abs_path}")
                    else:
                        print(f"[XAPP_LOG] ERROR: Missing timestamp or UE data in response")
                else:
                    print(f"[XAPP_LOG] WARNING: Unrecognized E2AP message from gNB {msg['meid']}")
        else:
            print("[XAPP_LOG] No messages received in this cycle")
        
        print("[XAPP_LOG] ------- End of processing cycle -------")


def e2sm_report_request_buffer():
    master_mess = RAN_message()
    master_mess.msg_type = RAN_message_type.INDICATION_REQUEST
    inner_mess = RAN_indication_request()
    inner_mess.target_params.extend([RAN_parameter.GNB_ID, RAN_parameter.UE_LIST, RAN_parameter.CELL_LOAD, RAN_parameter.TIMESTAMP])
    master_mess.ran_indication_request.CopyFrom(inner_mess)
    buf = master_mess.SerializeToString()
    return buf

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="xApp for collecting RAN metrics")
    parser.add_argument("-o", "--output-dir", help="Directory to save CSV output file")
    args = parser.parse_args()
    
    print(f"[XAPP_LOG] Starting xApp with output directory: {args.output_dir if args.output_dir else 'current directory'}")
    xappLogic(args.output_dir)