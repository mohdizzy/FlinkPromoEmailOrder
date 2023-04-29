# FlinkPromoEmailOrder

This repository contains a basic Flink app that processes a hypothetical e-commerce order stream containing all details for every purchase made by a customer in a given order. This app produces an output whenever it detects two specific product IDs ordered on the same day.

**Pre-requisites:**
 1. Install Maven
 2. Install Intellij (community edition is free)
 3. Other internal JDK dependencies can be automatically installed from the Intellij IDE.
 4. To build final .jar file, run **mvn clean package** in root directory (where pom.xml is present)

**Notes:**
 1. Each file contains inline comments explaining it's use and how things work internally. 
 2. The "job" class (program's entry point) has the configuration for deploying on AWS Kinesis Data Analytics. If you would like to deploy this app on your own Flink instance, the connectors and other setup will have to change.
 3. There is a unit test file that will allow to run the whole Flink app in your local system without needing to deploy anywhere.
 
**Deployment:**
 1. To deploy this app on AWS KDA, create an S3 bucket and drop the build file in there. 
 2. You will need a Kafka cluster with two topics created for source & sink. 
 3. Create a Flink KDA instance and point to the created S3 bucket. 
 4. The property map containing the details of the topics and other settings should be setup as well (*see Job class file*).
 5. After the app is in "running" status, open the Flink dashboard on AWS. If everything goes well, you should see the app with two operators.