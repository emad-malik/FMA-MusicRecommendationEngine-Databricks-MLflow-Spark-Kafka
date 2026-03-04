# FMA-MusicRecommendationEngine
Phase 1: Capacity Planning (The Engineering Baseline)
Before writing a single line of code, we need to define what the system must survive in production. Let us assume we are building a backend for a music platform where users can upload snippets of tracks to get instant genre tags.

Total Users: 50,000 Daily Active Users.

Concurrent Users: At peak hours, expect about 5% to be active, so around 2,500 concurrent connections.

Requests per User: Average of 5 requests per day.

Total Throughput: 250,000 requests per day (roughly 3 to 10 requests per second at peak).

Payload Size: A 30-second MP3 snippet is roughly 1 MB. Peak ingress is about 10 MB/second.

Latency Target: Under 800 milliseconds per user from upload to genre prediction.

Processing audio is incredibly CPU-intensive. Decoding an MP3, extracting the features (like MFCCs or spectrograms), and running a deep learning model inference takes time. If we rely on a basic web server, it will instantly bottleneck.

Phase 2: Architectural Evolution
Here is how the architecture scales from a basic concept to an enterprise-grade big data pipeline.

Step 1: The Basic Monolith (Why it fails)
You build a FastAPI endpoint. A user uploads an audio file, and the server decodes it using Librosa, runs the PyTorch or TensorFlow model, and returns the genre.
With 10 concurrent users, the CPU maxes out. Request number 11 waits in a queue until it times out. This design is tightly coupled, blocks the main thread, and crashes under pressure.

Step 2: Decoupling with Apache Kafka (Scalable Ingestion)
To fix the bottleneck, we introduce an event-driven architecture.

The user uploads the audio to an S3 bucket (or local MinIO storage) and sends a lightweight request to the API with the file's ID.

The API instantly publishes a message to a Kafka topic named raw_audio_events and returns a "Processing" status to the user.

Kafka acts as a massive shock absorber. Even if 1,000 users upload files simultaneously, Kafka holds the messages safely.

A pool of independent worker nodes subscribes to the Kafka topic, pulls the files, processes the audio, and pushes the genre prediction to a completed_predictions topic.

The API is never blocked, and the system can handle massive traffic spikes seamlessly.

Step 3: Apache Spark for Big Data Processing (Scalable Compute)
Kafka handles the real-time traffic, but what about the 93 GiB of historical data? You cannot preprocess 106,000 MP3 files on a single machine to train your initial model. It would take weeks.

This is where Apache Spark steps in.

Batch Preprocessing: You write a Spark job that distributes the workload across a cluster of nodes. Spark reads the 93 GiB of MP3s from your storage, decodes them in parallel, extracts the Mel-spectrograms, and saves the engineered features back to storage in a highly optimized format like Parquet.

Distributed Training: Spark MLlib or Spark's integration with deep learning frameworks can be used to train your genre classification model across the cluster, cutting training time from days to hours.

Phase 4: The Final MLOps Pipeline
Now we tie it all together with the tools you are already mastering.

Continuous Integration (Jenkins): You push a new PySpark feature engineering script to GitHub. Jenkins catches the commit, runs unit tests to ensure your audio extraction logic is mathematically sound, and triggers the big data pipeline.

Model Training & Tracking (MLflow): Jenkins orchestrates the Spark cluster to train the new deep learning model. MLflow tracks every hyperparameter (like spectrogram window size or CNN dropout rates) and logs the heavy model artifacts.

Champion vs. Challenger: Jenkins pulls the new model and compares its F1-score on a validation set against the current production model. If it wins, it is registered in MLflow.

Streaming Inference: Your Kafka consumer workers load the newly promoted model from MLflow into memory and begin processing the live stream of user uploads.
