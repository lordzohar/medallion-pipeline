# Medallion pipeline

# Modern Data Engineering with the Medallion Pipeline
## Student Package - Ubuntu On-Prem Labs

This package contains the issued learning material for the 10-day course. Each day has one slide deck, one lab guide, a quick recap, and only the files needed to perform that day's exercises.

## How to Use This Package

1. Complete the days in order. Concepts and lab difficulty deliberately build from local files to messaging, CDC, processing, orchestration, monitoring and a capstone.
2. Open `Slides/Day_XX_Slides.pptx` before beginning the lab.
3. Follow `Lab/Day_XX_Lab_Guide.docx`; it is the single issued lab instruction document for that day.
4. Use files in `Lab_Files/` from a Linux terminal on your Ubuntu lab machine.
5. Read `Recap/Day_XX_Recap.docx` at the end of the session and write down one point you can now explain without notes.

## Course Progression

| Day | Core idea | Practical focus |
| --- | --- | --- |
| 01 | Data engineering foundations and medallion thinking | Python batch vs event-flow labs and notebooks |
| 02 | Kafka foundations | Producers, consumers, brokers, topics and partitions |
| 03 | Reliable event pipelines | Kafka practice that prepares for change-data capture |
| 04 | Change data capture | PostgreSQL changes through Debezium to Kafka |
| 05 | Batch transformation | Bronze, Silver and Gold processing patterns |
| 06 | Visual pipeline engineering | Apache Hop-style pipeline design and operations |
| 07 | Spark lakehouse concepts | MinIO/Kafka environment and Spark/Iceberg design |
| 08 | Orchestration | Airflow DAG, sensors, tasks, retries and idempotency |
| 09 | Monitoring and incident analysis | Observable streaming workload and recovery reasoning |
| 10 | Integrated capstone | CDC, Kafka and ksqlDB pipeline demonstration |

## Student Folder Convention

Every day uses the same folders:

- `Slides`: the issued lesson deck.
- `Lab`: the single instruction document you should follow.
- `Lab_Files`: runnable code, compose files, data and setup files.
- `Recap`: a brief revision document.

Day 01 also includes `Notebooks` for guided practice.

## Important Scope Notes

- Use the exact local services named in each day's lab guide. Optional production tools discussed in class are not automatically installed in every exercise.
- The course is designed for local Ubuntu practice. Docker services may require significant memory; shut down services at the end of a lab.
- Keep screenshots, command output and short observations for your learning record and Day 10 capstone evidence.

## Suggested Daily Evidence

Submit or retain:

1. A screenshot of the final successful pipeline state or result.
2. One command output or query result that proves your data moved or changed.
3. Three sentences: what happened, how you verified it, and what could fail in production.
