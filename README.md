# Kafka Stream app for cat (Scala)

This app is calcualting an index for the air quality in the room of the sensors. This air quality is calculated by referencing the air quality of the windowed mean of the last 24h. To be able to not trigger false alarms too often, the windowsed mean of the last 10 minustes are used to compare the air quality to the last 24h. 
 
## Open window detection

Due to seasons it was not possible to detect the opening of the windows with only the temperature sensor. The open windows detection, tries to detect sudden changes in the last 10 minutes.