# Chatbot-SCPRS

## Project Description
This project integrates a Large Language Model (LLM) with a purchase orders dataset, allowing users to query and analyze the data through natural language conversations. The chatbot can understand and respond to questions about the dataset, providing insights and analysis based on the purchase order records.

## Prerequisites
- Docker and Docker Compose
- Postman (or any API testing tool)
- Git

## Setup Instructions

### 1. Clone the Repository
```bash
git clone git@github.com:hozRifai/chatbot-SCPRS.git
cd chatbot-SCPRS
```

### 2. Create the .env file, It should have 4 envs:
```bash
MONGO_ROOT_USERNAME=user
MONGO_ROOT_PASSWORD=password
MONGO_DATABASE=your database name
OPENAI_API_KEY=your openai api key
```

### 3. Start the Docker Containers
Run the following command in the project root directory:
```bash
docker-compose up -d
```

This will start two containers:
- MongoDB container for data storage
- Chatbot container with the LLM integration

### 4. Load the Dataset
After the containers are running, load the dataset into MongoDB using the following API endpoint:

```bash
curl --location --request POST 'http://localhost:8000/load-data'
```

You can either import the code above directly to Postman using Import button, or:
1. Create a new POST request
2. Enter URL: `http://localhost:8000/load-data`
3. Send the request

## Using the Chatbot

To interact with the chatbot, send POST requests to the chat endpoint. Here's an example:

```bash
curl --location 'http://localhost:8000/chat' \
--header 'Content-Type: application/json' \
--data '{
    "message": "how many orders had REQ0011118 as their requisition number where the creation date was in 2013?"
}'
```

Using Postman:
1. Create a new POST request
2. Enter URL: `http://localhost:8000/chat`
3. Set header: `Content-Type: application/json`
4. Add request body:
   ```json
   {
       "message": "your question here"
   }
   ```

## Example Questions
You can ask questions about the purchase orders dataset such as:
- "How many orders were created in 2013?"
- "What are the most common requisition numbers?"
- "Show me orders with specific requisition numbers"
- "What's the date range of the orders?"

## Project Structure
```
chatbot-SCPRS/
├── docker-compose.yml
├── dataset/
│   └── purchase.csv
├── mongodb/
│   └── persistent data here
├── chatbot/
│   └── [source files]
└── README.md
```

## Troubleshooting
- If containers don't start, check if the required ports (8000 and 27017) are available
- If data loading fails, ensure MongoDB container is healthy
- For any connection issues, verify both containers are running: `docker ps`

## Contact
For issues and questions, please open an issue in the GitHub repository.
