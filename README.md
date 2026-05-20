# ⚓ Miniraft

A simplified implementation of the Raft consensus algorithm built using Node.js and Docker. The project simulates a distributed system with multiple replicas, a gateway service, and a frontend interface to demonstrate leader-based coordination and fault-tolerant request handling.

---

## 🚀 Project Overview

Miniraft is an educational distributed systems project inspired by the Raft consensus algorithm. It consists of three replica nodes that emulate distributed state replication, a gateway that forwards client requests, and a frontend for interacting with the cluster.

The project demonstrates key concepts such as:

- Leader-based coordination
- Request routing through a gateway
- Replication across nodes
- Containerized deployment using Docker Compose
- Fault tolerance simulation

---

## 🏗️ Architecture


Frontend (Browser)
        │
        ▼
Gateway Service
        │
 ┌──────┼──────┐
 ▼      ▼      ▼
Replica1 Replica2 Replica3

✨ Features:
🧠 Simplified Raft-inspired distributed architecture
🔁 Multiple replica nodes
🌐 Gateway-based request routing
🐳 Fully containerized with Docker
⚡ Easy startup using Docker Compose
📚 Educational demonstration of distributed systems concepts

🛠️ Tech Stack:
Backend
Node.js
Express.js
Frontend
HTML
JavaScript
DevOps
Docker
Docker Compose

📁 Project Structure:
miniraft/
├── docker-compose.yml
├── frontend/
│   ├── Dockerfile
│   └── index.html
├── gateway/
│   ├── Dockerfile
│   ├── index.js
│   └── package.json
├── replica1/
│   ├── Dockerfile
│   ├── index.js
│   └── package.json
├── replica2/
│   ├── Dockerfile
│   ├── index.js
│   └── package.json
└── replica3/
    ├── Dockerfile
    ├── index.js
    └── package.json
    
⚙️ Getting Started
Prerequisites:
Docker
Docker Compose

Install Docker Desktop:
https://www.docker.com/products/docker-desktop/

🚀 Running the Project:
1. Clone the Repository
git clone https://github.com/reddyvaishnavii/miniraft.git
cd miniraft
2. Start All Services
docker-compose up --build
3. Open the Application

Open your browser and visit:

http://localhost:8080

Note: The exact port may vary depending on your docker-compose.yml configuration.

🧪 How It Works:
The frontend sends requests to the gateway.
The gateway forwards requests to the active replica.
Replica nodes process and replicate the request.
Responses are returned to the frontend.

This setup simulates how distributed nodes coordinate and maintain consistency.

🎯 Learning Outcomes:

Through this project, I gained hands-on experience with:

Distributed systems fundamentals
Raft consensus concepts
Microservice architecture
Docker containerization
Inter-service communication
Fault-tolerant design principles

👩‍💻 Contributors:
Vaishnavi Reddy
Team Members and Collaborators

📄 License:

This project is intended for educational and demonstration purposes.
