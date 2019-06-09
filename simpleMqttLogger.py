"""
 -- Simple MQTT logger v.01  --
 
The program will subscribe to the MQTT mqttBroker topic and logg all messages received
using internal buffers and multithreading to optimize the data collection procedure.

Files will be creaged under the logg directory with the following format yymmddHHMMSS-mqttMessages.log

The threading is optimized for low CPU consumption.

optional arguments:
  --help         Show this help message
  -h MQTTHOST    MQTT host address (default=localhost)
  -p MQTTPORT    MQTT port number (default=1883)
  -u MQTTUSER    MQTT username
  -P MQTTPASS    MQTT password
  -t MQTTTOPIC   MQTT topic (default=All toypics)
  -l LOGSFOLDER  Logg directory (default=data-logs)
  -r NUMLOGS     Number of logss per file (default=unlimited)
  -nl            Add new line char on every mqtt messages
  -ats           Append timestamp to saved mqtt message
  -at            Append topic to saved mqtt message
  -v             Show program verbose output

"""

import paho.mqtt.client as mqtt
import argparse
import time
import logging
import threading, os
try:
   import queue
except ImportError:
   import Queue as queue

# --------------------------------------------------------------------------------

argDescription = """ -- Simple MQTT logger v.01 - JavierFG --  
The program will subscribe to the MQTT mqttBroker topic and logg all messages received 
using internal buffers and multithreading to optimize the data collection procedure.
Files will be creaged under the logg directory with the following format yymmddHHMMSS-mqttMessages.log"""

parser = argparse.ArgumentParser( description=argDescription, formatter_class=argparse.RawDescriptionHelpFormatter, add_help=False)
parser.add_argument("--help", action="help", default=argparse.SUPPRESS, help=argparse._("Show this help message"))
parser.add_argument("-h", help="MQTT host address (default=localhost)", type=str, default="localhost", dest="mqttHost")
parser.add_argument("-p", help="MQTT port number (default=1883)", type=int, default=1883, dest="mqttPort")
parser.add_argument("-u", help="MQTT username", default=None, dest="mqttUser")
parser.add_argument("-P", help="MQTT password", default=None, dest="mqttPass")
parser.add_argument("-t", help="MQTT topic (default=All toypics)", default="#", dest="mqttTopic")
parser.add_argument("-l", help="Logg data MQTT messages directory (default=data-logs)", default="data-logs", dest="logsFolder")
parser.add_argument("-r", help="Number of logss per file (default=unlimited)", type=int, default=None, dest="numLogs")

parser.add_argument("-nl", help="Add new line char on every mqtt messages", dest="newLine", action="store_true")
parser.add_argument("-ats", help="Append timestamp to saved mqtt message", dest="addTime", action="store_true")
parser.add_argument("-at", help="Append topic to saved mqtt message", dest="addTopic", action="store_true")
parser.add_argument("-v", help="Show program verbose output", dest="verbose", action="store_true")

args = parser.parse_args()

#------------------------------------------------------------------------------------------------------------------
# USER FUNCTION to parse some info and display it.
def mqttMessageUserActions ( mqttMsg):
    """
    User function to perform some action once the mqtt message has been received.
    Do no load this function too much, only use it to display some info from the mqtt message.

    :param mqttMsg: class MQTTMessage. This is a class with members topic, payload, qos, retain
    """
    try:
        logging.info(" Message received in \"" + str(mqttMsg.topic) + "\" ( " + str(len(mqttMsg.payload)) + " bytes )")
    except:
        pass

#------------------------------------------------------------------------------------------------------------------
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info(" Connected to MQTT , waiting for messages . . .")
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(args.mqttTopic)
    else:
        logging.error(" Could not connect to MQTT Broker !")
        programFinish() #kill the thread and exit the program

# The callback when DISCONNECTED from server.
def on_disconnect(client, userdata, msg):
    logging.error("  Disconnected from the server !")
    if runThreadWorker:
        logging.error("  Lost connection - Trying to reconnect")
        client.reconnect()

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    #logging.debug("  MQTT msg received, queue it")
    qThread.put(msg)
    eventThread.set()

def logFile_Create ():

    global f

    fileFolder = args.logsFolder
    fileName = time.strftime("%y%m%d%H%M%S") + "-mqttMessages.log"

    #Create the folder
    try:
        if not os.path.exists(fileFolder):
            os.makedirs(fileFolder)
    except:
        raise
    #Open the file
    filePath = fileFolder + "/" + fileName

    try:
        f = open(filePath,'a')
    except:
        raise
    logging.debug("  File created: " + filePath )
    return True

def logFile_Close():
    logging.debug("  Closing file")
    try:
        f.close()
    except:
        pass

    return True

def logFile_AppendMqttData(msgMQTT):
    try:
        if args.newLine:
            if(counterMsgs >= 1): f.write("\n")
        if args.addTime: f.write( "%.2f"%time.time() + ";")

        try:
            if args.addTopic: f.write( str(msgMQTT.topic) + ";")
        except:
            logging.error("  Could not save message, non utf-8 characters used!")
            return

        try:
            f.write(msgMQTT.payload.decode("utf-8", "replace"))
        except:
            logging.error("  Could not save message from %s, non utf-8 characters used!"%str(msgMQTT.topic))
            return

    except OSError:
        raise
    #logging.debug("   Datta written to file " + str(len(msgMQTT.payload)) + " bytes")
    return True

def logFile_FlushData ():
    try:
        f.flush()
    except:
        pass

def threadWorker():

    global counterMsgs

    logging.debug("  Thread started")
    counterMsgs = 0
    while runThreadWorker:

        eventThread.wait()  #block thread

        while not qThread.empty():
            msgMQTT = qThread.get()
            logFile_AppendMqttData(msgMQTT)
            mqttMessageUserActions(msgMQTT)
            counterMsgs += 1
            if not args.numLogs == None:
                if counterMsgs >= args.numLogs: logFile_Close(); logFile_Create(); counterMsgs = 0

        logFile_FlushData()
        eventThread.clear()
    logging.debug("  Thread stopped")

def programFinish ():

    global runThreadWorker

    runThreadWorker = False  # exit the thread

    try:
        eventThread.set()  # force to run
    except:
        pass

    time.sleep(1)  # wait to close it

    try:
       client.disconnect()  # Disconnect MQTT
    except:
        pass

    logFile_Close()  # Close the file

    logging.info("Program finished!")

    quit()  # Finish the script

#=======================================================================

levelLogging = logging.INFO
if args.verbose: levelLogging = logging.DEBUG

logging.basicConfig(format="[%(asctime)s] %(message)s", level=levelLogging, datefmt="%y-%m-%d %H:%M:%S")

logging.info(" -- MQTT simple logger starting - v0.1 --")
logging.info("INFO: to stop the program press CTR+C on the terminal window")

# MQTT connection --------------------------
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.username_pw_set( args.mqttUser, args.mqttPass)

try:
    logging.error("Connecting to MQTT mqttBroker . . .")
    client.connect(args.mqttHost, args.mqttPort, 60)
except Exception as e:
    logging.error("  Could not connect to MQTT mqttBroker : " + e.strerror)
    programFinish()
	
# Logg file --------------------------------
if not logFile_Create():
    logging.error("Could not create the log file, check file permissions")
    programFinish()

# Thread for the message processing ----------

qThread = queue.Queue()

eventThread = threading.Event()
eventThread.clear()

runThreadWorker = True
t = threading.Thread( target= threadWorker)
t.start()

# Main program ------------------------------
try:
    while runThreadWorker :
        client.loop_forever()
except KeyboardInterrupt:
    logging.warning("Program interrrupted by keyboard")

#Finished-------------------------------------------------------------------------------

programFinish()     #close everything
