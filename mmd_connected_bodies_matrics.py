from array import array
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
import argparse
import datetime
import json
import time
import os

#############################################################################################################
######################### Update the value of below array before running the script #########################
# Value of the below array need to set manually for each env before running the script 
# These value will be mapped to time specified in "time_matrix" variable
# Below value should not contain zero
# Below are dummy value for testing
count_matrix_normal = array('l', [5, 10, 15, 20, 25])

# If current value is more then below specified percentage then it will treat this as error.
THRESHOLD_PERCENTAGE_CHANGE = 50  
#############################################################################################################


# Wait time to start consumer
CONSUMER_START_TIME_IN_SEC = 5
# Wait time for each consumer pool to get some event
CONSUMER_TIMEOUT_IN_SEC = 2
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
SCRIPT_START_TIME = datetime.datetime.utcnow().strftime(TIME_FORMAT)

# 5mins, 15mins, 1hour, 4hours, 24hours
time_matrix = array('l', [5*60, 15*60, 1*60*60, 4*60*60, 24*60*60]) 
count_matrix = array('l', [0, 0, 0, 0, 0])

def getBroker(dc):
    return "kafka." + dc + ".tivo.com:9092"

def getInputTopic(dc, env):
    return dc + "." + env + ".frontend.connectedBodies"

def get_latest_offset(broker, topic, offsets):
    consumer = KafkaConsumer(topic,
                             bootstrap_servers = '%s' %(broker),
                             consumer_timeout_ms = (CONSUMER_START_TIME_IN_SEC * 1000),
                             enable_auto_commit = False, 
                             auto_offset_reset = 'latest',
                             max_poll_records = 1)
    # print ("Message consuming from topic to find latest offset [%s]." % (topic))

    partitions = consumer.partitions_for_topic(topic)
    # Initialize the consumer other wise seek will failed with error "Unassigned 
    consumer.poll(CONSUMER_TIMEOUT_IN_SEC * 1000 , 1)

    for p in partitions:
        topic_partition = TopicPartition(topic, p)
        # move the offset to end of the topic
        consumer.seek_to_end(topic_partition)
        offset = consumer.position(topic_partition);
        # Subtract offset by one, because position() return offset of the next record that will be fetched
        if offset > 0:
            offset = offset - 1;
        # print ("Partition = %s, Last Offset = %d" % (p, offset))
        offsets.insert(p, offset);

    consumer.close(1)

def processTimeDiff(delta_time_sec):
    # print("delta_time_sec = %i time_matrix = %s" % (delta_time_sec, time_matrix))
    if (delta_time_sec <= time_matrix[0]):                                            # <5 mins
        count_matrix[0] += 1
    elif ((time_matrix[0] < delta_time_sec) and (delta_time_sec <= time_matrix[1])):  # 5-15 mins
        count_matrix[1] += 1
    elif ((time_matrix[1] < delta_time_sec) and (delta_time_sec <= time_matrix[2])):  # 15-60 mins
        count_matrix[2] += 1
    elif ((time_matrix[2] < delta_time_sec) and (delta_time_sec <= time_matrix[3])):  # 1-4 hours
        count_matrix[3] += 1
    elif ((time_matrix[3] < delta_time_sec) and (delta_time_sec <= time_matrix[4])):  # 4-24 hours
        count_matrix[4] += 1
    # elif ((time_matrix[4]) < delta_time_sec):                                         # > 1 day
    #    count_matrix[5] += 1
        
# Function to consume events
def connected_body_topic_consumer(broker, in_topic):
    # Get the latest offsets 
    end_offsets = array("i")
    get_latest_offset(broker, in_topic, end_offsets)

    consumer = KafkaConsumer(bootstrap_servers='%s' % (broker),
                             consumer_timeout_ms=(CONSUMER_START_TIME_IN_SEC * 1000),
                             enable_auto_commit=False,
                             auto_offset_reset='earliest',
                             max_poll_records=2)
    partitions = consumer.partitions_for_topic(in_topic)

    for p in partitions:
        end_ptn_loop = False
        topic_partition = TopicPartition(in_topic, p)
        consumer.assign([topic_partition])
        # print ("Partition [%d] end offset [%d]" % (p, end_offsets[p]))

        try:
            while True:
                if end_ptn_loop == True:
                    break
                consumer.poll(CONSUMER_TIMEOUT_IN_SEC * 1000 , 1)
                for msg in consumer:
                    # print ("Reading message, Partition : [%s] Offset : [%s] Key : [%s] : LastOffset [%i]" % (msg.partition, msg.offset, msg.key, end_offsets[p]))
                    # print ("Reading message, Partition : [%s] Offset : [%s] Key : [%s] Value:  [%s]" % (msg.partition, msg.offset, msg.key, msg.value))
                    msg_str = str(msg.value)
                    msg_body_start_index = msg_str.index('{')
                    msg_body = msg_str[msg_body_start_index:-1]
                    # print ("Reading message, Partition : [%s] Offset : [%s] Key : [%s] Value:  [%s]" % (msg.partition, msg.offset, msg.key, msg_body))
                    #json_msg = json.dumps(msg_body)
                    json_obj = json.loads(msg_body)
                    # print ("Reading message, Partition : [%s] Offset : [%s] Key : [%s] Connected time:  [%s]" % (msg.partition, msg.offset, msg.key, json_obj['connectedBody']['connectedTime']))
                    tcd_connected_time = json_obj['connectedBody']['connectedTime'];
                    #print (tcd_connected_time)
                    
                    script_start_time = SCRIPT_START_TIME
                    delta_time = datetime.datetime.strptime(script_start_time, TIME_FORMAT) - datetime.datetime.strptime(tcd_connected_time, TIME_FORMAT)
                    # print (delta_time)
                    delta_time_sec = delta_time.total_seconds()
                    # print (delta_time_sec)
                    
                    processTimeDiff(delta_time_sec)
                                        
                    if msg.offset >= end_offsets[p]:
                        end_ptn_loop = True
                        break

        except Exception as ex:
            print ("Exception during consuming message : ", str(ex))
            
    consumer.close()
    # print ("\nEnd of consumer method\n")
    return (1)
    
def connectionStatus(i):
    if (((count_matrix[i] - count_matrix_normal[i]) / count_matrix_normal[i]) * 100) > THRESHOLD_PERCENTAGE_CHANGE:
        return "ERROR"
    else:
        return "OK"

def printConnetionMatrix():
    i = 0
    print ("|-----------------------|--------------------|------------------------|--------------------|")
    print ("| Age of connection     |   No of Boxes in   | No of boxes connected  |     % deviation    |")
    print ("|                       | Normal conditions  | while running script   |     OK / ERROR     |")
    print ("|-----------------------|--------------------|------------------------|--------------------|")
    while i < len(count_matrix):
        print ("| Less then {:6.0f} mins |     {:10.0f}     |  {:9.0f}             |        %s          |".format(time_matrix[i]/60, count_matrix_normal[i], count_matrix[i]) % connectionStatus(i))
        i += 1
    print ("|-----------------------|--------------------|------------------------|--------------------|\n")


######################### Main ########################
if __name__ == '__main__':
    parser = argparse.ArgumentParser("\nMiddlemind connected bodies metrics. \n"
                                     "    python3 mmd_connected_bodies_matrics.py --dc <DC> --env <ENV>\n"
                                     "    Before running this script please set the value of \"count_matrix_normal\" variable by editing the script file")
    parser.add_argument('--dc', help='Datacenter name', required=True)
    parser.add_argument('--env', help='Environment name', required=True)
    args = parser.parse_args()
    dc = args.dc
    env = args.env

    broker = getBroker(dc)
    in_topic = getInputTopic(dc, env)
    print("Started reading from connected bodies topic = [%s]" %(in_topic))
    print("Script start time = " + SCRIPT_START_TIME)
    # print("Script start time = " + datetime.datetime.strptime(SCRIPT_START_TIME, TIME_FORMAT))

    print ("\nPlease wait... ")
    print ("It can take around 30mins, depending on the topic size.\n")

    try:
        # Start Consumer
        connected_body_topic_consumer(broker, in_topic)

        printConnetionMatrix()

    except Exception as e:
        print ("Exception in script : ", str(e))

