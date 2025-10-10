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

    # Determine CSV data directory
    if not output_dir:
        # If output directory is not specified, create a subdirectory named "ran_metrics" in the current directory
        output_dir = os.path.join(os.getcwd(), "ran_metrics")
    
    # Ensure directory exists
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"[XAPP_LOG] Created data directory: {output_dir}")
        else:
            print(f"[XAPP_LOG] Using existing data directory: {output_dir}")
    except Exception as e:
        print(f"[XAPP_LOG] Error: Cannot create directory {output_dir}: {str(e)}")
        # If directory creation fails, fall back to current directory
        output_dir = os.getcwd()
        print(f"[XAPP_LOG] Falling back to current directory: {output_dir}")
    
    # Create a timestamped CSV filename
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = os.path.join(output_dir, f"ran_metrics_{current_time}.csv")
    print(f"[XAPP_LOG] Saving data to file: {csv_filename}")
    
    fieldnames = ['timestamp', 'gnb_id', 'rnti', 'rsrp', 'bler_ul', 'bler_dl', 'mcs_ul', 'mcs_dl', 'dl_prbs_total', 'ul_prbs_total']
    
    # Create new CSV file and write header row
    with open(csv_filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        print(f"[XAPP_LOG] Created CSV file and wrote header row")

    # subscription requests
    for gnb in gnb_id_list:
        e2sm_buffer = e2sm_report_request_buffer()
        connector.send_e2ap_sub_request(e2sm_buffer, gnb)
        print(f"[XAPP_LOG] Sent subscription request to gNB {gnb}")
    
    # read loop with 500ms interval
    sleep_time = 0.5  # 500ms
    print(f"[XAPP_LOG] Starting data collection loop, interval: {sleep_time*1000}ms")
    
    loop_count = 0
    while True:
        loop_count += 1
        sleep(sleep_time)
        messgs = connector.get_queued_rx_message()
        
        # Print loop status periodically (about every 10 seconds)
        if loop_count % 20 == 0:
            print(f"[XAPP_LOG] Running - Loop {loop_count}, data storage path: {csv_filename}")
        
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
                    ue_list = None
                    
                    for param in resp.param_map:
                        if param.key == RAN_parameter.TIMESTAMP:
                            timestamp = param.timestamp
                        elif param.key == RAN_parameter.UE_LIST:
                            ue_list = param.ue_list

                    if timestamp and ue_list:
                        # Format timestamp for display
                        timestamp_str = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        ue_count = len(ue_list.ue_info)
                        print(f"[XAPP_LOG] Timestamp: {timestamp_str}, UEs reported: {ue_count}")
                        
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
                                        'bler_ul': ue.bler_ul,
                                        'bler_dl': ue.bler_dl,
                                        'mcs_ul': ue.mcs_ul,
                                        'mcs_dl': ue.mcs_dl,
                                        'dl_prbs_total': ue.dl_prbs_total,
                                        'ul_prbs_total': ue.ul_prbs_total
                                    }
                                    writer.writerow(row)
                                    ue_count += 1
                                    print(f"[XAPP_LOG] UE {ue.rnti}: RSRP={ue.rsrp}dBm, BLER UL={ue.bler_ul}, BLER DL={ue.bler_dl}, MCS UL={ue.mcs_ul}, MCS DL={ue.mcs_dl}, DL PRBs={ue.dl_prbs_total}, UL PRBs={ue.ul_prbs_total}")
                                
                                # Only display save status when data volume is large or periodically, to avoid excessive logging
                                if ue_count >= 3 or loop_count % 10 == 0:
                                    print(f"[XAPP_LOG] Saved data for {ue_count} UEs to file")
                        except Exception as e:
                            print(f"[XAPP_LOG] Error: Failed to write to CSV file: {str(e)}")
                        else:
                            # Periodically display file information
                            if loop_count % 20 == 0:
                                file_size = os.path.getsize(csv_filename) / 1024  # KB
                                print(f"[XAPP_LOG] Data file: {os.path.basename(csv_filename)}, Size: {file_size:.2f} KB")
                    else:
                        print(f"[XAPP_LOG] Error: Missing timestamp or UE data in response")
                else:
                    print(f"[XAPP_LOG] Warning: Unrecognized E2AP message from gNB {msg['meid']}")
        else:
            # Reduce log output when no messages are received
            if loop_count % 20 == 0:
                print("[XAPP_LOG] No messages received in this cycle")
        
        # Only print cycle end marker on specific loops or when messages are received
        if len(messgs) > 0 or loop_count % 20 == 0:
            print("[XAPP_LOG] ------- Processing cycle ended -------")


def e2sm_report_request_buffer():
    master_mess = RAN_message()
    master_mess.msg_type = RAN_message_type.INDICATION_REQUEST
    inner_mess = RAN_indication_request()
    inner_mess.target_params.extend([RAN_parameter.GNB_ID, RAN_parameter.UE_LIST, RAN_parameter.TIMESTAMP])
    master_mess.ran_indication_request.CopyFrom(inner_mess)
    buf = master_mess.SerializeToString()
    return buf

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="xApp for collecting RAN metrics")
    parser.add_argument("-o", "--output-dir", help="Data storage directory (default: ./ran_metrics)")
    args = parser.parse_args()
    
    print(f"[XAPP_LOG] Starting xApp...")
    xappLogic(args.output_dir)
