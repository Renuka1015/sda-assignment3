# Streaming Data Analytics – Ride-Hailing Streaming Pipeline

## Project Overview

This project demonstrates an end-to-end **Streaming Data Analytics pipeline** for a ride-hailing business. Synthetic ride-event data is read from a CSV file, streamed through **Apache Kafka**, consumed using Python, stored in **MongoDB Atlas**, and visualized through a **MongoDB Atlas Charts management dashboard**.

The dashboard is designed from a management perspective, focusing on ride demand, fare generation, service mix, pricing pressure, and customer experience.

## Architecture

```text
sample_data.csv
      |
      v
Python Producer (producer.py)
      |
      v
Apache Kafka - ride_events
      |
      v
Python Consumer (consumer.py)
      |
      v
MongoDB Atlas
SDA_Streaming / ride_events
      |
      v
MongoDB Atlas Charts
      |
      v
Management Dashboard
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.13 | Producer and consumer |
| Apache Kafka | Event streaming |
| Apache Zookeeper | Kafka coordination |
| Docker / Docker Compose | Kafka and Zookeeper |
| kafka-python | Python-Kafka integration |
| MongoDB Atlas | Cloud data storage |
| PyMongo | Python-MongoDB integration |
| MongoDB Atlas Charts | Dashboard and visualization |
| CSV | Sample source data |
| VS Code | Development |

## Project Structure

```text
SDA_Streaming_Project/
|
├── venv/                  # Local Python virtual environment
├── docker-compose.yml     # Kafka and Zookeeper configuration
├── requirements.txt       # Python dependencies
├── sample_data.csv        # Synthetic ride-event data
├── producer.py            # Kafka producer
├── consumer.py            # Kafka consumer + MongoDB loader
└── README.md              # Project documentation
```

> The `venv/` directory should normally be excluded from GitHub using `.gitignore`.

## Data Fields

| Field | Description |
|---|---|
| `ride_id` | Ride/event identifier |
| `city` | City in which the ride occurs |
| `ride_type` | Ride/service category |
| `event_type` | Event status/type |
| `distance_km` | Ride distance |
| `fare` | Fare value |
| `surge_multiplier` | Pricing multiplier |
| `payment_status` | Payment status |
| `rating` | Customer rating |
| `timestamp` | Event timestamp generated during streaming |

# Setup

## 1. Prerequisites

Install:

- Python 3.13
- Docker Desktop
- MongoDB Atlas account
- VS Code (recommended)

Verify Python:

```bash
python --version
```

Verify Docker:

```bash
docker --version
```

## 2. Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd SDA_Streaming_Project
```

## 3. Create the Python Environment

Windows:

```cmd
py -3.13 -m venv venv
venv\Scripts\activate
```

## 4. Install Dependencies

```cmd
pip install -r requirements.txt
```

Dependencies:

```text
kafka-python==3.0.1
pymongo
dnspython
```

# Kafka Configuration

## 5. Start Kafka and Zookeeper

```cmd
docker compose up -d
```

Check the services:

```cmd
docker compose ps
```

This project uses:

```text
Kafka:      localhost:9093
Zookeeper:  localhost:2182
```

## 6. Create the Kafka Topic

```cmd
docker exec -it sda-project-kafka /opt/kafka/bin/kafka-topics.sh --create --topic ride_events --bootstrap-server localhost:9093 --partitions 1 --replication-factor 1
```

Verify:

```cmd
docker exec -it sda-project-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9093
```

The list should include:

```text
ride_events
```

# MongoDB Atlas Configuration

The consumer stores events in:

```text
Database:   SDA_Streaming
Collection: ride_events
```

Configure an Atlas database user and appropriate network access, then obtain the Atlas connection string.

In `consumer.py`, replace:

```python
MONGODB_URI = "PASTE_YOUR_MONGODB_CONNECTION_STRING_HERE"
```

with your connection string.

**Security:** Never commit a real MongoDB password or private connection string to a public GitHub repository. Use environment variables/secrets before publishing the repository.

# Running the Pipeline

## 7. Start the Producer

Open a terminal:

```cmd
cd SDA_Streaming_Project
venv\Scripts\activate
python producer.py
```

The producer reads `sample_data.csv` and continuously sends JSON ride events to:

```text
Kafka topic: ride_events
```

Example:

```text
Starting Uber ride event producer...
Sending events to Kafka topic: ride_events

Sent: {'ride_id': 'R001', ...}
Sent: {'ride_id': 'R002', ...}
```

## 8. Start the Consumer

Open another terminal:

```cmd
cd SDA_Streaming_Project
venv\Scripts\activate
python consumer.py
```

The consumer:

1. Connects to Kafka.
2. Reads events from `ride_events`.
3. Converts the JSON message to a Python object.
4. Inserts the event into MongoDB Atlas.

Example:

```text
Connecting to MongoDB Atlas...
MongoDB Atlas connection successful!
Connecting to Kafka...
Kafka connection successful!
Listening for ride events...

Inserted: R001 | Delhi | Uber Go | Fare: 245.0
Inserted: R002 | Mumbai | Uber Auto | Fare: 180.0
```

The completed pipeline is:

```text
CSV → Python Producer → Kafka → Python Consumer → MongoDB Atlas
```

# Management Dashboard

The dashboard is designed around managerial questions rather than simply visualizing available fields.

## Key Questions

- How much ride activity is being generated?
- What gross fare value is being captured?
- What is the average fare per ride?
- Where is ride demand concentrated?
- Which ride types contribute more to gross fare value?
- Where is pricing pressure visible?
- How does trip distance relate to fare?
- How does customer experience vary across cities?

## Recommended Dashboard Components

### 1. Total Ride Events
Count of `ride_id`.

**Purpose:** Overall ride activity captured by the streaming system.

### 2. Total Gross Fare
Sum of `fare`.

**Purpose:** Gross fare value in the observed streaming data.

> This is not profit because the dataset does not contain operating costs, driver payouts, or platform costs.

### 3. Average Fare per Ride
Average of `fare`.

**Purpose:** Average monetary value of an observed ride.

### 4. Average Customer Rating
Average of `rating`.

**Purpose:** High-level customer experience indicator.

### 5. Ride Demand by City
Count of `ride_id` grouped by `city`.

**Purpose:** Identifies where ride activity is concentrated and can support supply and operational planning.

### 6. Gross Fare Contribution by Ride Type
Sum of `fare` grouped by `ride_type`.

**Purpose:** Shows the observed gross fare contribution of different service categories.

### 7. Surge Multiplier vs Fare
Compare `surge_multiplier` with `fare`.

**Purpose:** Helps management examine pricing pressure and the relationship between surge levels and fare values.

### 8. Trip Distance vs Fare
Scatter plot using `distance_km` and `fare`.

**Purpose:** Helps assess how fare values vary with trip distance.

### 9. Average Customer Rating by City
Average `rating` grouped by `city`.

**Purpose:** Allows comparison of customer experience across operating markets.

## Management Decision Framework

```text
Streaming Data
      ↓
Visualization
      ↓
Business Insight
      ↓
Management Investigation
      ↓
Operational Decision
```

Examples:

| Observation | Possible Management Use |
|---|---|
| High ride concentration in a city | Review supply and operational capacity |
| High fare contribution from a ride type | Monitor service mix and availability |
| Higher surge associated with higher fares | Examine demand-supply and pricing patterns |
| Lower rating in a high-volume city | Investigate customer experience |
| Longer trips associated with higher fares | Evaluate pricing patterns by trip distance |

The dashboard identifies patterns and areas for managerial investigation; it does not automatically establish causality.

# Data Limitations

The project uses **synthetic sample data** to demonstrate the streaming architecture.

The current dataset does not contain:

- Driver ID
- Driver availability
- Driver payout
- Operating cost
- Platform commission
- Customer wait time
- Cancellation duration
- Profit

Therefore, the dashboard does not directly calculate true profit, driver utilization, customer waiting time, cancellation rate, or platform margin.

# Stopping the Project

Stop the Python producer or consumer with:

```text
Ctrl + C
```

Stop Kafka and Zookeeper:

```cmd
docker compose down
```

Restart them:

```cmd
docker compose up -d
```

# Project Outcome

The project demonstrates an end-to-end streaming analytics workflow:

```text
Sample Data
    ↓
Kafka Producer
    ↓
Apache Kafka
    ↓
Kafka Consumer
    ↓
MongoDB Atlas
    ↓
MongoDB Atlas Charts
    ↓
Management Dashboard
```

The final solution demonstrates how streaming operational data can be transformed into management-oriented information for monitoring demand, fare generation, service mix, pricing patterns, and customer experience.


