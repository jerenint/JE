#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 17:00:11 2020

https://github.com/justeat/JustEat.RecruitmentTest/blob/master/Test.Data.Engineer.md

Generate event pairs for a given number of orders, at a specified time interval
in seconds which gives out JSON extracts in batches of the number of events
specified at location path.

Steps to follow:
    - Create 4/5 orders with OrderPlaced to OrderDelivered and 1/5 to OrderCancelled
    - Create OrderId using random uuid generator then attach to event pair
    - Create Timestamp 
    - Save to location as a line-delimited JSON file with name orders-<timestamp>.json


Runtime parameters:
generate-events \
    --number-of-orders=1000000 \
    --batch-size=5000 \
    --interval=1 \
    --output-directory=<local-dir>
    
 Generates JSON files based on the number of orders, batch size, interval (s) 
 of creation between each file and the end location specified.
    
 Example:
python GenerateEvents.py --num_of_orders 10 --batch_size 5 --interval_seconds 1 --output_directory './'

@author: jerenitolentino
"""


import uuid
import datetime
import json
import time
import argparse
import os

class GenerateEvents:
    
    def __init__(self, num_of_orders, batch_size, interval, local_dir):
        """
        Initialises the input parameters
        """
        self.num_of_orders = int(num_of_orders)
        self.batch_size = int(batch_size)
        self.interval = int(interval)
        self.local_dir = str(local_dir)
        # Create list to store the events
        self.extract = []

    def create_events(self,i):
        """
        Generates the paired events per order where the first event must be an
        order placed followed by an OrderCancelled or OrderDelivered.
        """
        
        # Create new paired events for each order and store them in the list
        # of events to write
        event_1 = {}
        event_2 = {}
        event_pair = []
        new_uuid = str(uuid.uuid1())
        date_time = datetime.datetime.now().isoformat()
        event_1["Data"] = {"OrderId":new_uuid,"TimestampUtc":date_time}
        event_1["Type"] = "OrderPlaced"
        event_2["Data"] = {"OrderId":new_uuid,"TimestampUtc":date_time}
        if i % 5 == 0:
            event_2["Type"] = "OrderCancelled"
        else:
            event_2["Type"] = "OrderDelivered"
        event_pair.append(event_1)
        event_pair.append(event_2)
        return event_pair

        
    def open_file(self):
        """
        Returns the final JSON file name for each file generated.
        """
        output_time = datetime.datetime.now().isoformat('-').replace(':','-').replace('.','-')
        final_path = self.local_dir + 'orders-' + output_time + '.json'
        output_file = open(final_path, 'w')
        return output_file
    
    def write_to_location(self):
        """
        Writes each of the events on a new line to the output directory
        """
        
        output_write = self.open_file()
        for event in self.extract[:self.batch_size]:
            json.dump(event, output_write) 
            output_write.write("\n")
        output_write.close()
        
    def events_to_file(self):
        self.write_to_location()
        # Reset the list to include events remaining that have not been
        # written to file
        self.extract = self.extract[self.batch_size:] 
        time.sleep(self.interval)
                
    def main(self):
        """
        JSON file created for the number of orders under a certain batch size.
        """
        
        # For the specified number of orders, create paired events
        for i in range(self.num_of_orders):
            self.extract.extend(self.create_events(i))
            # When the number of the extract is divisible by the batch size,
            # write the events to the JSON file at specified time interval.
            if int(len(self.extract)/self.batch_size)>0:
                # Reset the list to include events remaining that have not been
                # written to file
                self.events_to_file()
        # For any remaining events write to file even if it does not reach
        # specified batch size.
        if len(self.extract) != 0:
            self.events_to_file()

        
        
def parse_arguments():
    """
    Parses the input arguments to generate events based on the optional input
    parameters passed. 
    Checks if directory location is valid
    """
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_of_orders", type=int, 
                        help="Number of orders you would like to generate",default=1000000)
    parser.add_argument("--batch_size", type=int, 
                        help="Number of events per JSON file",default=5000)
    parser.add_argument("--interval_seconds", type=int, 
                        help="Time interval between the creation of each JSON file",default=1)
    parser.add_argument("--output_directory", type=str, 
                        help="Output location of the JSON files",default='./')
    args = parser.parse_args()
    
    if not (os.path.isdir(args.output_directory)):
        parser.error('Invalid directory')
    return args



if __name__ == "__main__":
    params = parse_arguments()
    gen_events = GenerateEvents(params.num_of_orders,params.batch_size,params.interval_seconds,params.output_directory)  
    gen_events.main()
        
    



