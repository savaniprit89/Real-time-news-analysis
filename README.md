# Real-Time News Named Entity Extraction using PySpark, Kafka, and spaCy

This project builds a **real-time streaming pipeline** for processing news articles using **Apache Kafka**, **PySpark Structured Streaming**, and **spaCy**.

The system works in two stages:

1. A **producer** fetches live news article content from NewsAPI and pushes it to a Kafka topic.
2. A **consumer/stream processor** reads those news messages from Kafka, performs **Named Entity Recognition (NER)** using spaCy, counts the extracted entities, and publishes the results to another Kafka topic.

---

## Project Overview

This project demonstrates:

- Real-time news ingestion
- Kafka-based message streaming
- PySpark Structured Streaming
- Named Entity Recognition (NER) with spaCy
- Entity frequency counting
- Sending processed results back to Kafka

---

## Architecture

```text
NewsAPI
   ↓
Kafka Producer
   ↓
Kafka Topic: Assignment_3
   ↓
PySpark Streaming Consumer
   ↓
spaCy NER Extraction
   ↓
Entity Counting
   ↓
Kafka Topic: Result
