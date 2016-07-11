README

EgaDownloadService.jar  **PROTOTYPE**

EGA Download Service: Given a valid download ticket (from a valid IP) this service will collect data associated with download ticket, create the re-encrypted resource in the ReEncryption Service, and streams the re-encrypted stream to the client.

Startup: 

	java -jar EgaDownloadService.jar [-l, -f, -p, -t]

"java -jar EgaDownloadService.jar" starts the service using default configuration (Port 9127). 
	Parameter '-l' allows to specify a different path to the database config XML file
	Parameter '-f' allows to specify a different name of the database config XML file
	Parameter '-p' allows to specify a different port for the service to listen
	Parameter '-t' performs a self-test of the service

Startup as service:

	nohup java -jar EgaDownloadService.jar > download.log 2>&1&

Defaults - Port: 9127, Config File: 'DownloadEcosystem.xml', Config Path: './../headers/'

The service creates a new directory "dailylog". In this directory there is a log file that lists all requests sent to this service. A new log file is created at the beginning of each new day.

------------

This service logs downloads in a database. A "Succesful" download means that the file has been streamed from the archive through the Downoad service with matching sizes and MD5 checksums. The server can't enforce verifying that the user MD5 matches the sent MD5 - this is an optional step to be performed by the server. 

------------

Project is created using Netbeans.

Netbans uses ant, so it can be built using ant (version 1.8+). Ant target "package-for-store" creates the packaged Jar file containing all libraries. There are no further dependencies, everything is packaged in the Jar file.

Servers use Netty as framework. Client REST calls use Resty.

------------

The service runs HTTP only. It is intended that way.

------------

API
 Base URL: /ega/rest/download/v1

 /ega/rest/download/v1/downloads/{downloadticket}?ip={ip}  Start downloading a re-encrypted file
 /ega/rest/download/v1/results/{downloadticket}?ip={ip}  gets the MD5 and size of the data sent, after download
 /ega/rest/download/v1/stats/load                   returns server CPU load
 /ega/rest/download/v1/stats/hits[?minutes=minutes] returns # of hits (approximate) within the past {minutes} minutes
 /ega/rest/download/v1/stats/avg[?minutes=minutes]  returns response time/avg of hits (approximate) within the past {minutes} minutes

