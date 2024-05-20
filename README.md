# Weather-Stations-Monitoring

DDIA course project about a distributed system that fetches weather information from multiple sources, archives and visualizes it.

![img1](https://github.com/015525/Data_Intensive_project/assets/95590176/e72846fe-b43f-49c7-9848-6f75048c03b1)


## Agenda
- [System Architecture](#system-architecture)
- [How to Run](#how-to-run)

<a name="-system-architecture"></a> 
## System Architecture
There are 3 major components implemented using 6 microservices. </br>
The 3 major components are:
1) [Data Acquisition](#data-acquisition).
2) [Data Processing & Archiving](#data-processing-and-archiving).
3) [Data Indexing](#data-indexing).

<a name="-data-acquisition"></a> 
## Data Acquisition
Multiple weather stations that feed a queueing service (Kafka) with their readings.</br>
### A) Weather Stations
- We have 2 types of stations: **Mock Stations**, and **Adapter Stations**.
- Mock Stations are required to randomise its weather readings.
- Adapter Stations get results from Open-Meteo according to a latitude and longitude given at the beginning.
- Both types will have the same **battery distribution(30% low - 40% medium - 30% high)** and dropping percentage of **10%**.
- We use a **station factory design pattern** to indicate which type of station we would like to build.
- We use a **builder design pattern** to build the message to be sent.
- Messages coming from Open Meteo are brought according to the **Adapter and filter integration pattern**.
![img 2](https://github.com/015525/Data_Intensive_project/assets/95590176/5e1b33aa-e017-4e9f-883e-b9f90c22a384)




### B) Kafka Processors
- There are two types of processing following **router integration pattern**:
  
| Processing Type | Description |
| --- | --- |
| Dropping Messages | Processes messages by some probabilistic sampling, then throw some of them away |
| Raining Areas | Processes messages and detects rain according humidity, then throw messages from rainy areas to some topic. We use Kstream and filters to do such processors |

- All these processing produce undropped messages to **weather_topic** And all records have **humidity >= 70** go to **raining topic**.


<a name="-data-processing-and-archiving"></a> 
## Data Processing And Archiving
- This is done in 2 stages:

| Point | Description |
| --- | --- |
| 1) Data Collection and Archiving | Where data is consumed from Kafka topics then written in batches of 10k messages in the form of parquet files to support efficient analytical queries. |
| 2) Parquet files Partioning | Where a **Chronological job** scheduled to run every 24 hours at midnight **with the help of k8s** and **implemented in Scala using Apache Spark** to partition messages in parquet files by **both day and station_id**. |
    
<a name="-data-indexing"></a> 
## Data Indexing
This is composed of two parts: </br>
a) [BitCask Storage](#bitcask-storage) </br>
b) [Elastic Search and Kibana](#elastic-search-and-kibana) </br>

<a name="-bitcask-storage"></a> 
## Bitcask Storage
- We implemented the BitCask Riak LSM to maintain an updated store of each station status as discussed in [the book](https://drive.google.com/file/d/120RsgrUsgNFkg1hChAG05LI6LS09sM-p/view?usp=drive_link) with some notes:
  *  **Scheduling compaction** over segment files to avoid disrupting active readers.
  *  **No checksums implemented** to detect errors.
  *  **No tombstones implemented** for deletions as there is no point in deleting some weather station ID entry. We just deleted Replica files on Compaction.
  *  here is the structure of entry in segment files: </br>

      | **ENTRY:** | timestamp | key_size | value_size | key| value |
      | --- | --- | --- | --- | --- | --- |
      | **SIZE:** | 8 bytes | 4 bytes | 4 bytes | key_size | value_size |
    
  *   ### Classes
        | Class | Description |
        | --- | --- |
        | BitCask | Provides API for writing, reading keys, and values. |
        | ActiveSegment | Handles the currently active segment file. |
        | Compaction Task | Contains the following two runnable classes and uses a timer schedule to run them periodically. |
        | Compact |  Used to compact segments periodically. |
        | deleteIfStale | Used to delete a file if it's marked as a stale file (a file that was compacted). |
        | BitCaskLock | Used to ensure one writer at a time for the active file. |
        | SegmentsReader | Given the hash table entry and file segment path, returns the value corresponding to this key. |
        | SegmentsWriter | Given key, value, segment file path, writes the key, value into the segment. |
        | Pointer | Points to an entry and contains two attributes: filePath, ByteOffset. |

  * ### Crash Recovery Mechanism
    - Create a new hashMap.
    - Read Active file, start to end, and add key, pointer to value pairs to hashTable.
    - Read hint files, from end to start, and fill hashTable with key value pairs.
  * ### Compaction Mechanism
    - Loop on all replica files, read each replica file from start to end, add its key value pairs to hashMap.
    - Loop on hashTable, write each key value as entry in a compacted file. 
    - Mark Replica File as **Stale**.
    - **Another scheduled task deleteIfStale will delete file** replica file **if it’s STALE file**. 
  * ### MultiWriter concurrency Mechanism
    Implemented using **WriteLockFile**, to allow for concurrency over multiple bitCask Instances, on the same shared Storage. 
    - When Writer acquires lock, **WriteLockFile** is created in segments path.
    - When another writer wants to write to active file, it **checks** for **WriteLockFile**, if it **exists**, it **waits until file is deleted**.
    - When the other **writer finishes**, it **deletes WriteLock file**. 
 

<a name="-elastic-search-and-kibana"></a> 
## Elastic Search and Kibana
- We implemented a **Python script** listening to kafka topic (paths_topic) where the base central station **sends paths of newly-created parquet files**.
- The **script then a loads a parquet file** and converts it to records and **connects to elasticsearch to upload records**.
- Here is the Kibana visualisations confirming Battery status distribution of some stations confirming the battery distribution of stations:
![img 3](https://github.com/015525/Data_Intensive_project/assets/95590176/163b1a17-a766-462f-a28d-9feaed28a0dd)


- Here is also Kibana visualisations calculating the percentage of dropped messages from stations confirming the required percentage 10%:
![img 4](https://github.com/015525/Data_Intensive_project/assets/95590176/b2f65601-b4af-445b-8a1b-d977359188a2)

