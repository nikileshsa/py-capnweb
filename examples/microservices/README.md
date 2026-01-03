# Microservices Example

Demonstrates a microservices architecture using Cap'n Web with capability-based security.

## Features

- Service mesh architecture with API gateway
- Capability-based authentication
- Cross-service authorization
- Role-based access control (admin vs user)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Client                               │
└──────────────────────┬──────────────────────────────────────┘
                       │ HTTP
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    API Gateway (Port 8080)                   │
│                                                              │
│  Methods:                                                    │
│  - login(username, password) → {token, userId, role}         │
│  - getUserProfile(token) → {userId, username, email, role}   │
│  - createOrder(token, items) → {orderId, total, status}      │
│  - listOrders(token) → [{orderId, total, status}, ...]       │
└──────────────┬──────────────────────────┬───────────────────┘
               │                          │
               │ HTTP                     │ HTTP
               ▼                          ▼
┌──────────────────────────┐  ┌──────────────────────────────┐
│   User Service (8081)    │  │   Order Service (8082)       │
│                          │  │                              │
│  - authenticate()        │  │  - createOrder(user, items)  │
│  - getUser(id)           │  │  - listOrders(user)          │
│  - getUserByToken(token) │  │  - getOrder(id)              │
└──────────────────────────┘  └──────────────────────────────┘
```

## Running

### Terminal 1 - User Service

```bash
uv run python examples/microservices/user_service.py
```

### Terminal 2 - Order Service

```bash
uv run python examples/microservices/order_service.py
```

### Terminal 3 - API Gateway

```bash
uv run python examples/microservices/api_gateway.py
```

### Terminal 4 - Demo Client

```bash
uv run python examples/microservices/client.py
```

## Key Concepts

### Capability Passing Between Services

The API gateway receives a token, converts it to a User capability via the
User Service, then passes that capability to the Order Service for authorization.

### Cross-Service Permission Verification

The Order Service calls methods on the User capability to verify permissions
before performing operations.

### Role-Based Access Control

- **Users**: Can create orders, view own orders
- **Admins**: Can do everything users can, plus cancel any order

## Available Users

| Username | Password | Role  |
|----------|----------|-------|
| alice    | alice123 | admin |
| bob      | bob123   | user  |
| charlie  | charlie123 | user |
