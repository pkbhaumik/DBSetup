## Database Service (Partial)

Setup Database for Ticket Service REST API. 

Release expired hold tickets

## Prequisite

Java 8

SQL Express 2017 with SQL Server authentication enabled

Eclipse

Maven

Apache Kafka 1.0.1 (Install Apache ZooKeeper And Apache Kafka)

## Setup

1. Create a SQL Server login
2. Run the TicketService.sql from dbscripts folder to create the database.
3. Provide read/write permission to SQL Server user

## Build

Go to the pb-ticket-db-service folder and run the following command

mvn package

## Run the program

java -jar pb-ticket-db-service-jar-with-dependencies.jar {SQL_INSTANCE_NAME} {DATABASE_NAME} { USER_NAME} {PASSWORD} {KAFKA_BROKERS}

e.g.

java -jar pb-ticket-db-service-jar-with-dependencies.jar 127.0.0.1\SQLEXPRESS TicketService ticketservice Ticketservice123 127.0.0.1:9092


