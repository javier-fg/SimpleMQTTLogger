# SimpleMQTTLogger-Python

# -- Simple MQTT logger v.01  --
 
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
