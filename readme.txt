---
title: IdGraphUserManagementMs
version: 4.0.4
last_updated: 2026-08-03
author: IDGraph Platform Team
---

# IdGraphUserManagementMs — User Lifecycle Management Microservice

| Field | Value |
|---|---|
| **Domain / Team** | `IDGraph Platform` |
| **Artifact ID** | `IDGraphUserManagementMs` |
| **Version** | `0.0.2-SNAPSHOT` |
| **Component Type** | `REST API (JAX-RS) + Kafka + SOAP + MQ` |
| **Runtime / Parent** | `sdk-java-parent 3.0.1` |
| **Language / Java Version** | `Java 17` |
| **Base Path / Entry Point** | `/msapi/idgraphusermanagementms/v1` |
| **API / Contract Docs** | `Swagger UI — /idgraphusermanagementms/swagger/index.html` |

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Project Structure](#project-structure)
4. [Domain Capability Map](#domain-capability-map)
5. [Integration Context](#integration-context)
6. [Process Flows](#process-flows)
7. [Interfaces & Endpoints](#interfaces--endpoints)
8. [Request / Response Contracts](#request--response-contracts)
9. [Key Classes and Code References](#key-classes-and-code-references)
10. [Data Stores & State Management](#data-stores--state-management)
11. [Async Messaging (JMS / IBM MQ)](#async-messaging-jms--ibm-mq)
12. [External Integrations](#external-integrations)
13. [Configuration](#configuration)
14. [Dependencies](#dependencies)
15. [Build and Test](#build-and-test)
16. [Deployment](#deployment)
17. [Workflows](#workflows)
18. [Observability and Operations](#observability-and-operations)
19. [Security and Compliance](#security-and-compliance)
20. [Change Log](#change-log)

---

## Overview

`IdGraphUserManagementMs` is a Spring Boot microservice within the AT&T Identity Graph (IDGraph) platform responsible for the full user lifecycle — registration, profile updates, password management, member ID linking, access ID merging, username changes, user deletion, and fraud lock operations.

**Key capabilities:**

- **User Registration** — Add new users with optional temporary passwords and auto-generated access IDs; register email-based OPR users
- **User Profile Management** — Update user profiles, contact CBR data, and external user records
- **Password Operations** — Update, reset, and manage prepaid user passwords for access IDs and linked member IDs
- **Access ID / Username Management** — Check availability, change usernames, check merge eligibility, merge access IDs
- **Member ID Operations** — Link/unlink member IDs to access IDs (SLIDs), add and upgrade secondary member IDs, query primary/secondary member ID details
- **User Deletion** — Delete access IDs or member IDs when all linked services are removed
- **Fraud Management** — Update user fraud lock details
- **Credential Validation** — Validate user credentials against the MPSYS system
- **Profanity Scanning** — Check input strings for bad/profanity words
- **Transaction Rate Limiting** — IP-based transaction rate-limit verification
- **Registration Orchestration** — Orchestrate registration association processing across services via `POST /orchestration/registration`
- **Async Event Publishing** — Publish lifecycle events to IBM MQ queues for downstream consumption
- **MS-to-MS Integration** — REST calls to IDGraphUserProfileMs, IDGraphUserAssociationMs, OrderGraph, and others
- **SOAP Integration** — MergeSLIDProfile calls via WSDL-generated JAXB stubs

**Technology stack:** Java 17, Spring Boot 3.x (Seed SDK 3.0.1), Jersey/JAX-RS, JPA/Hibernate, Oracle DB, IBM MQ, Azure App Configuration, Voltage encryption, OpenAPI 3.

---

## Architecture

```mermaid
graph TD
    subgraph Clients[Calling Systems / Clients]
        REST[HTTP/JSON REST]
    end

    subgraph UserMgmtMs[IdGraphUserManagementMs]
        subgraph Interface[JAX-RS Resource Layer - 12 Resources]
            R1[UserProfileResourceImpl]
            R2[RegisterUserResourceImpl]
            R3[MemberResourceImpl]
            R4[UserPasswordResourceImpl]
            R5[DeleteUserResourceImpl]
            R6[Other Resources - 7]
        end
        subgraph Validation[Request Validators - 24]
            V[RequestValidator per endpoint]
        end
        subgraph Services[Service Layer - 25 Impls]
            S[ServiceImpl orchestration]
        end
        subgraph Helpers[Helper Layer]
            H[Business Logic Helpers - 34+]
            QH[Queue Message Helpers - 12]
        end
        subgraph DBLayer[DB Helper Layer - 35]
            DBH[Oracle JPA/Hibernate Helpers]
        end
    end

    subgraph External[External Dependencies]
        ORA[(Oracle DB)]
        MQ[IBM MQ / DBUS]
        EventHub[Azure Event Hub / Kafka]
        AuditKafka[Kafka Audit Topics]
        UPms[IDGraphUserProfileMs]
        UAms[IDGraphUserAssociationMs]
        OG[OrderGraph]
        MPS[MPS SOAP]
        Voltage[Voltage Server]
        KeyVault[Azure Key Vault]
        AppConfig[Azure App Configuration]
    end

    REST --> Interface
    Interface --> Validation
    Validation --> Services
    Services --> Helpers
    Helpers --> DBLayer
    Helpers --> QH
    DBLayer --> ORA
    QH --> MQ
    QH --> EventHub
    Helpers --> AuditKafka
    Helpers --> UPms
    Helpers --> UAms
    Helpers --> OG
    Helpers --> MPS
    Helpers --> Voltage
    UserMgmtMs --> KeyVault
    UserMgmtMs --> AppConfig
```

**Key architectural patterns:** Layered JAX-RS REST API, request validator gate on every endpoint, async IBM MQ + Azure Event Hub event publishing, Oracle XA DataSource with JPA/Hibernate, SOAP adapter for MPS, Spring Cloud Stream for Kafka-based event notifications, Azure App Configuration for dynamic config refresh.

---

## Project Structure

```
apm0045194-idgraph-idgraphusermanagementms/
├── pom.xml                                   # Maven build — parent: sdk-java-parent 3.0.1
├── src/
│   ├── main/
│   │   ├── java/com/att/idp/idgraphusermanagementms/
│   │   │   ├── Application.java              # Spring Boot entry point (@EnableAsync, @EnableRetry, @EnableCaching)
│   │   │   ├── appconfig/config/             # Azure App Configuration integration (5 classes)
│   │   │   ├── client/                       # SOAP handler (MPSMergeSLIDProfileSoapHandler)
│   │   │   ├── config/                       # Spring @Configuration classes (13 classes)
│   │   │   ├── db/helper/                    # Database helpers — 35 classes
│   │   │   ├── db/model/                     # JPA entity models
│   │   │   ├── db/repository/               # Spring Data repositories
│   │   │   ├── exceptions/                   # Exception mappers (5 classes)
│   │   │   ├── healthcheck/                  # DB + JMS health indicators
│   │   │   ├── helper/                       # Business logic helpers — 34 top-level classes
│   │   │   │   ├── resetuserpassword/        # Password reset helpers
│   │   │   │   └── voltage/                  # Voltage encryption helpers
│   │   │   ├── integration/                  # MPSYSInternalRestClient, RegistrationOrchestrationRestClient (MS-to-MS)
│   │   │   ├── message/                      # Message constants and DTOs
│   │   │   ├── queue/message/helpers/        # MQ queue message builders (12 classes)
│   │   │   ├── queue/message/sender/         # JMSQueueSender
│   │   │   ├── request/validator/            # Request validators (24 classes)
│   │   │   ├── resource/                     # JAX-RS interfaces (12) and impls (12)
│   │   │   ├── resource/request/             # Request DTOs (35 classes incl. nested types)
│   │   │   ├── resource/response/            # Response DTOs (25+ classes)
│   │   │   ├── service/                      # Service interfaces (25) and impls (25)
│   │   │   └── utils/                        # Utility classes (10)
│   │   └── resources/
│   │       ├── schemas/                      # WSDL/XSD for SOAP (MergeSLIDProfile)
│   │       ├── errormessages.properties       # Error message catalog
│   │       └── logmessages.properties         # Log message catalog
│   ├── test/
│   │   ├── groovy/                           # Spock BDD unit + component tests
│   │   ├── java/                             # JUnit 5 unit tests
│   │   └── ist/                              # Integration/IST test configs
│   └── resources/
├── opt/ajsc/etc/config/                      # Runtime config: application.properties, logback, ESAPI, Voltage
│   ├── application.properties
│   ├── userMgmt.properties
│   ├── logback.xml
│   ├── rilhrs-bad-word-data/                 # Profanity word lists (15 files)
│   └── security-filter-config.yaml
├── idpsecretsloader.yaml                     # Azure Key Vault secret loader config
├── Jenkinsfile                               # CI pipeline definition
└── README.md
```

---

## Domain Capability Map

| Capability | Description | Primary Interfaces / Entry Points |
|------------|-------------|-----------------------------------|
| **User Registration** | Add new users with optional temp password and auto-generated access ID; register OPR email users | `POST /user/add`, `POST /register/emailuser` |
| **User Profile Management** | Update user profiles, external users, and CBR contact data | `POST /user/update`, `POST /user/addExt`, `POST /user/updateExt`, `POST /contacts/user/update` |
| **Password Management** | Update, reset, and prepaid password operations | `POST /userpassword/update`, `/reset`, `/updatePrepaid` |
| **Username / Access ID Management** | Check availability, change username, check and execute merge eligibility | `POST /user/change`, `/checkUserIdAvailability`, `/checkEligibility`, `/merge` |
| **Member ID Operations** | Link/unlink SLIDs, add/upgrade secondary member IDs, query member ID details | `POST /member/link`, `/unlink`, `/secmemberid/add`, `/secmemid/upgrade`, `GET /memberid` |
| **User Deletion** | Delete access IDs or member IDs when all linked services are removed | `POST /deleteuser` |
| **Fraud Management** | Update user fraud lock details | `POST /user/updateFraud` |
| **Credential Validation** | Validate user credentials against MPSYS | `POST /user/credentials/validate` |
| **Profanity Scanning** | Check input strings against bad-word lists | `POST /profanity/scanner` |
| **Transaction Rate Limiting** | IP-based rate limit verification | `POST /register/txnratelimit/verify` |
| **Registration Orchestration** | Orchestrate registration association processing | `POST /orchestration/registration` |
| **Async Event Publishing** | Publish lifecycle events to IBM MQ for downstream DBUS consumption | 12 Queue Helpers → `JMSQueueSender` |

---

## Integration Context

| Interface / Dependency | Direction | Type | Purpose | Notes |
|------------------------|-----------|------|---------|-------|
| Calling Systems / Clients | Inbound | REST/JSON | Consumer of all 25 endpoints | `X-ATT-ClientId`, `idp-trace-id` headers required |
| `IDGraphUserProfileMs` | Outbound | REST | InquireProfile, InquireAssociatedAccessIds | Via `MPSYSInternalRestClient` |
| `IDGraphUserAssociationMs` | Outbound | REST | AddAssociation, RemoveAssociation, ChangeStatus | Via `MPSYSInternalRestClient` / `RegistrationOrchestrationRestClient` |
| `IDGraphUserManagementMs (OUD)` | Outbound | REST | CheckUserIdAvailability, AddUser (OUD path) | Via `MPSYSInternalRestClient` |
| `OrderGraph` | Outbound | REST | Order-related integrations | Via `MPSYSInternalRestClient` |
| `MPS (MergeSLIDProfile)` | Outbound | SOAP | Merge access IDs via WSDL-generated stubs | `MergeSlidProfileSoapHelper` + `MPSSOAPConfig` |
| `CustomerGraph Orchestration MS` | Outbound | REST | Customer profile lookups, secure account profile | `apiclient.rest.cg.*` via `CGClientHelpers` |
| Oracle DB | Outbound | JPA/Hibernate | User, member, service, and contact data persistence | XA DataSource, 35 DB helpers |
| IBM MQ / DBUS | Outbound | JMS | Async lifecycle event publishing | `JMSQueueSender` with hash-based routing + failover |
| Azure Event Hub | Outbound | Kafka (Spring Cloud Stream) | External event notifications (user lifecycle events) | Topic: `*-idgraph-external-events-v1`; dual-binder primary/secondary |
| Kafka (Audit) | Outbound | Kafka (Spring Kafka) | Audit watermarking and event tracking | Topics: `com.att.idp.audit.watermarking.*`, `com.att.idp.audit.tracking.*` |
| Voltage Server | Outbound | SDK | PII/SPI encryption and decryption | `idp-voltage-encrypt` / `idp-voltage-decrypt` |
| Azure Key Vault | Outbound | Secret Loader | Secrets loaded at startup via `idpsecretsloader.yaml` | `IdpSecretsCacheHelper` |
| Azure App Configuration | Outbound | Dynamic Config | Runtime config refresh for DataSource pool params | `IDGraphAzureAppConfig` + `ConfigChangeEventListener` |

---

## Process Flows

### Add User Flow

```mermaid
sequenceDiagram
    participant Client as Calling System
    participant Resource as UserProfileResourceImpl
    participant Validator as AddUserRequestValidator
    participant Service as AddUserServiceImpl
    participant Helper as AddUserHelper
    participant DB as Oracle DB
    participant Queue as JMSQueueSender (IBM MQ)

    Client->>Resource: POST /user/add (JSON)
    Resource->>Validator: validateRequest(headers, body)
    Validator-->>Resource: Validation result
    alt Invalid
        Resource-->>Client: HTTP 400 error response
    end
    Resource->>Service: addUser(request)
    Service->>Helper: addUser(hmInput)
    Helper->>DB: Persist new user record
    DB-->>Helper: Result
    Helper-->>Service: Result map
    Service->>Queue: Publish AddUser event (async)
    Service-->>Resource: AddUserResponse
    Resource-->>Client: HTTP 200 JSON response
```

### Username Change Flow

```mermaid
flowchart TD
    A[POST /user/change] --> B[ChangeUserNameRequestValidator]
    B --> C{Validate headers & body}
    C -->|Invalid| D[Return 400 error response]
    C -->|Valid| E[ChangeUserNameServiceImpl]
    E --> F[ChangeUserNameHelper]
    F --> G{checkAccessIdMergeEligibility}
    G -->|Not eligible| H[Return ERR_NOT_ELIGIBLE]
    G -->|Eligible| I[DB: Execute username change]
    I --> J{Success?}
    J -->|No| K[Return ERR_SYS_APPL]
    J -->|Yes| L[Publish ChangeUserName event to MQ]
    L --> M[Return success response]
```

### Delete User Flow

```mermaid
sequenceDiagram
    participant Client as Calling System
    participant Resource as DeleteUserResourceImpl
    participant Validator as DeleteUserRequestValidator
    participant Service as DeleteUserServiceImpl
    participant Helper as DeleteUserHelper
    participant DBHelper as DeleteUserCommonHelper
    participant DB as Oracle DB
    participant Queue as JMSQueueSender

    Client->>Resource: POST /deleteuser (JSON)
    Resource->>Validator: validate(headers, body)
    Validator-->>Resource: Validation result
    Resource->>Service: deleteUser(request)
    Service->>Helper: deleteUser(hmInput)
    Helper->>DBHelper: verify linked services removed
    DBHelper->>DB: Check/delete records
    DB-->>DBHelper: Result
    DBHelper-->>Helper: Result
    Helper-->>Service: Result map
    Service->>Queue: Publish DeleteUser event (async)
    Service-->>Resource: DeleteUserResponse
    Resource-->>Client: HTTP 200 JSON response
```

### Link Member Flow

```mermaid
flowchart TD
    A[POST /member/link] --> B[LinkMemberRequestValidator]
    B --> C{Validate headers & body}
    C -->|Invalid| D[Return 400 error response]
    C -->|Valid| E[LinkMemberServiceImpl]
    E --> F[LinkMemberHelper]
    F --> G[LinkMemberDBHelper: Persist link]
    G --> H{Success?}
    H -->|No| I[Return ERR_SYS_APPL]
    H -->|Yes| J[Publish LinkMember event to MQ]
    J --> K[Return success response]
```

### Merge Access ID Flow

```mermaid
flowchart TD
    A[POST /user/merge] --> B[MergeUserAccessIdRequestValidator]
    B --> C{Validate headers & body}
    C -->|Invalid| D[Return 400 error response]
    C -->|Valid| E[MergeUserAccessIdServiceImpl]
    E --> F[MergeUserAccessIdHelper]
    F --> G[MergeSlidProfileSoapHelper: SOAP call to MPS]
    G --> H{SOAP Success?}
    H -->|No| I[Return error response]
    H -->|Yes| J[DB: Merge records]
    J --> K[Return success response]
```

### General Request Processing Pattern

Every endpoint follows this layered pattern:

1. **Resource** receives the HTTP request
2. **RequestValidator** validates headers (`X-ATT-ClientId`, `idp-trace-id`) and body
3. **ServiceImpl** orchestrates business logic
4. **Helper** performs core operations (DB writes, encryption, external calls)
5. **DBHelper** executes database operations via JPA/Hibernate
6. **QueueHelper** prepares and publishes async events to IBM MQ via `JMSQueueSender`

---

## Interfaces & Endpoints

All endpoints are served under the base path: **`/msapi/idgraphusermanagementms/v1`**

### Common Request Headers

| Header | Required | Description |
|--------|----------|-------------|
| `X-ATT-ClientId` | Yes | Calling system / application identifier |
| `idp-trace-id` | Yes | IDP trace / session correlation identifier |
| `idpctx-appname` | No | Application name context |

### Endpoint Summary

| # | Method | Path | Description | Resource |
|---|--------|------|-------------|----------|
| 1 | POST | `/user/add` | Register a new user | `UserProfileResourceImpl` |
| 2 | POST | `/user/update` | Update an existing user profile | `UserProfileResourceImpl` |
| 3 | POST | `/user/checkUserIdAvailability` | Check access ID / username availability | `UserProfileResourceImpl` |
| 4 | POST | `/user/change` | Change a user's access ID / username | `UserProfileResourceImpl` |
| 5 | POST | `/user/checkEligibility` | Check access ID merge eligibility | `UserProfileResourceImpl` |
| 6 | POST | `/user/updateFraud` | Update user fraud lock details | `UserProfileResourceImpl` |
| 7 | POST | `/user/addExt` | Add an external user | `UserProfileResourceImpl` |
| 8 | POST | `/user/updateExt` | Update an external user | `UserProfileResourceImpl` |
| 9 | POST | `/user/credentials/validate` | Validate user credentials | `UserProfileResourceImpl` |
| 10 | POST | `/user/merge` | Merge one access ID into another | `UserProfileResourceImpl` |
| 11 | POST | `/register/txnratelimit/verify` | Verify transaction rate limit by IP | `RegisterUserResourceImpl` |
| 12 | POST | `/register/emailuser` | Register a new OPR email user | `RegisterUserResourceImpl` |
| 13 | POST | `/userpassword/update` | Update password for access ID + linked members | `UserPasswordResourceImpl` |
| 14 | POST | `/userpassword/reset` | Reset a user's password | `UserPasswordResourceImpl` |
| 15 | POST | `/userpassword/updatePrepaid` | Update prepaid user password | `UserPasswordResourceImpl` |
| 16 | POST | `/member/link` | Link a member ID to an access ID (SLID) | `MemberResourceImpl` |
| 17 | POST | `/member/unlink` | Unlink a member ID from an access ID (SLID) | `MemberResourceImpl` |
| 18 | POST | `/secmemberid/add` | Add a secondary member ID | `AddSecMemberIdResourceImpl` |
| 19 | POST | `/secmemid/upgrade` | Upgrade secondary member ID to primary | `UpgSecMemberIdResourceImpl` |
| 20 | GET | `/memberid` | Retrieve primary/secondary member ID details | `QueryMemberIdResourceImpl` |
| 21 | POST | `/contacts/user/update` | Update user contact CBR information | `UpdateUserContactCBRResourceImpl` |
| 22 | POST | `/emailalias` | Remove email aliases | `EmailAliasResourceImpl` |
| 23 | POST | `/deleteuser` | Delete an access ID or member ID | `DeleteUserResourceImpl` |
| 24 | POST | `/profanity/scanner` | Check for bad/profanity words | `ProfanityScannerResourceImpl` |
| 25 | POST | `/orchestration/registration` | Orchestrate registration association processing | `RegistrationOrchestrationResourceImpl` |

### User Profile APIs (`/user`) — `UserProfileResource`

**10 endpoints** managed by `UserProfileResourceImpl`.

#### 1. POST `/user/add` — Add User

| Property | Value |
|----------|-------|
| **Description** | Register a new user in the MPSYS system with optional temporary password and auto-generated access ID |
| **Request DTO** | `AddUserRequest` |
| **Response DTO** | `AddUserResponse` |
| **Validator** | `AddUserRequestValidator` |
| **Service** | `AddUserServiceImpl` → `AddUserHelper` |
| **DB Helpers** | `DBHelper`, `AddMemberDBHelper`, `AddServiceDBHelper`, `GetSlidInfoDBHelper` |
| **MQ Event** | `AddUserQueueHelper` → publishes user registration event |
| **External Calls** | `MPSYSInternalRestClient` (OUD AddUser, CheckUserIdAvailability) |

#### 2. POST `/user/update` — Update User

| Property | Value |
|----------|-------|
| **Description** | Update an existing user profile in the MPSYS system |
| **Request DTO** | `UpdateUserRequest` |
| **Response DTO** | `UpdateUserResponse` |
| **Validator** | `UpdateUserRequestValidator` |
| **Service** | `UpdateUserServiceImpl` → `UpdateUserHelper` |
| **MQ Event** | `UpdateUserQueueHelper` → publishes user update event |

#### 3. POST `/user/checkUserIdAvailability` — Check User ID Availability

| Property | Value |
|----------|-------|
| **Description** | Check if an access ID / username is available for registration |
| **Request DTO** | `CheckUserIdAvailabilityRequest` |
| **Response DTO** | `CheckUserIdAvailabilityResponse` |
| **Validator** | `CheckUserIdAvailabilityRequestValidator` |
| **Service** | `CheckUserIdAvailabilityServiceImpl` → `CheckUserIdAvailabilityHelper` |

#### 4. POST `/user/change` — Change User Name

| Property | Value |
|----------|-------|
| **Description** | Change a user's access ID / username in the MPSYS system |
| **Request DTO** | `ChangeUserNameRequest` |
| **Response DTO** | `ChangeUserNameResponse` |
| **Validator** | `ChangeUserNameRequestValidator` |
| **Service** | `ChangeUserNameServiceImpl` → `ChangeUserNameHelper` |
| **MQ Event** | `ChangeUserNameQueueHelper` → publishes username change event |

#### 5. POST `/user/checkEligibility` — Check Access ID Merge Eligibility

| Property | Value |
|----------|-------|
| **Description** | Check whether two access IDs are eligible to be merged |
| **Request DTO** | `CheckAccessIdMergeEligibilityRequest` |
| **Response DTO** | `CheckAccessIdMergeEligibilityResponse` |
| **Validator** | `CheckAccessIdMergeEligibilityRequestValidator` |
| **Service** | `CheckAccessIdMergeEligibilityServiceImpl` → `CheckAccessIdMergeEligibilityHelper` |

#### 6. POST `/user/updateFraud` — Update User Fraud

| Property | Value |
|----------|-------|
| **Description** | Update user fraud lock details in the MPSYS system |
| **Request DTO** | `UpdateUserFraudRequest` |
| **Response DTO** | `UpdateUserFraudResponse` |
| **Validator** | `UpdateUserFraudRequestValidator` |
| **Service** | `UpdateUserFraudServiceImpl` |
| **DB Helpers** | `FraudLockDBHelper`, `MemberFraudPasswordDBHelper` |
| **MQ Event** | `UpdateUserFraudQueueHelper` → publishes fraud update event |

#### 7. POST `/user/addExt` — Add External User

| Property | Value |
|----------|-------|
| **Description** | Add an external user in the MPSYS system |
| **Request DTO** | `ExtUserRequest` |
| **Response DTO** | `ExtUserResponse` |
| **Validator** | `ExtUserRequestValidator` |
| **Service** | `AddExtUserServiceImpl` → `AddExtUserHelper` |

#### 8. POST `/user/updateExt` — Update External User

| Property | Value |
|----------|-------|
| **Description** | Update an external user in the MPSYS system |
| **Request DTO** | `ExtUserRequest` |
| **Response DTO** | `ExtUserResponse` |
| **Validator** | `ExtUserRequestValidator` |
| **Service** | `UpdateExtUserServiceImpl` → `UpdateExtUserHelper` |

#### 9. POST `/user/credentials/validate` — Validate User Credentials

| Property | Value |
|----------|-------|
| **Description** | Validate user credentials against the MPSYS system |
| **Request DTO** | `ValidateUserCredentialsRequest` |
| **Response DTO** | `ValidateUserCredentialsResponse` |
| **Validator** | `ValidateUserCredentialsRequestValidator` |
| **Service** | `ValidateUserCredentialsServiceImpl` |

#### 10. POST `/user/merge` — Merge User Access ID

| Property | Value |
|----------|-------|
| **Description** | Merge one access ID into another in the MPSYS system (includes SOAP call to MPS) |
| **Request DTO** | `MergeUserAccessIdRequest` |
| **Response DTO** | `MergeUserAccessIdResponse` |
| **Validator** | `MergeUserAccessIdRequestValidator` |
| **Service** | `MergeUserAccessIdServiceImpl` → `MergeUserAccessIdHelper` |
| **SOAP Call** | `MergeSlidProfileSoapHelper` → MPS MergeSLIDProfile WSDL |

### Registration APIs (`/register`) — `RegisterUserResource`

**2 endpoints** managed by `RegisterUserResourceImpl`.

#### 11. POST `/register/txnratelimit/verify` — Transaction Rate Limit Verify

| Property | Value |
|----------|-------|
| **Description** | Verify transaction rate limit status based on IP address |
| **Request DTO** | `TransactionRateLimitRequest` |
| **Response DTO** | `TransactionRateLimitResponse` |
| **Validator** | `TxnRateLimitStatusRequestValidator` |
| **Service** | `TxnRateLimitStatusServiceImpl` → `TxnRateLimitStatusHelper`, `IPTxnRateLimitHelper` |
| **DB Helpers** | `IPRateLimitDBHelper` |

#### 12. POST `/register/emailuser` — Register Email User

| Property | Value |
|----------|-------|
| **Description** | Register a new OPR email user |
| **Request DTO** | `RegisterEmailUserRequest` |
| **Response DTO** | `RegisterEmailUserResponse` |
| **Validator** | `RegisterEmailUserRequestValidator` |
| **Service** | `RegisterEmailUserServiceImpl` → `RegisterEmailUserHelper` |
| **MQ Event** | `RegisterEmailUserQueueHelper` → publishes email registration event |

### Password Management APIs (`/userpassword`) — `UserPasswordResource`

**3 endpoints** managed by `UserPasswordResourceImpl`.

#### 13. POST `/userpassword/update` — Update User Password

| Property | Value |
|----------|-------|
| **Description** | Update password for an access ID and its linked member IDs |
| **Request DTO** | `UpdateUserPasswordRequest` |
| **Response DTO** | `UpdateUserPasswordResponse` |
| **Validator** | `UpdateUserPasswordRequestValidator` |
| **Service** | `UpdateUserPasswordServiceImpl` |
| **MQ Event** | `PasswordQueueHelper` → publishes password change event |

#### 14. POST `/userpassword/reset` — Reset User Password

| Property | Value |
|----------|-------|
| **Description** | Reset a user's password in the MPSYS system |
| **Request DTO** | `ResetUserPasswordRequest` |
| **Response DTO** | `ResetUserPasswordResponse` |
| **Validator** | `ResetUserPasswordRequestValidator` |
| **Service** | `ResetUserPasswordServiceImpl` |
| **MQ Event** | `PasswordQueueHelper` → publishes password reset event |

#### 15. POST `/userpassword/updatePrepaid` — Update Prepaid User Password

| Property | Value |
|----------|-------|
| **Description** | Update prepaid user password for the access ID |
| **Request DTO** | `UpdatePrepaidUserPasswordRequest` |
| **Response DTO** | `UpdatePrepaidUserPasswordResponse` |
| **Validator** | `UpdatePrepaidUserPasswordRequestValidator` |
| **Service** | `UpdatePrepaidUserPasswordServiceImpl` |

### Member Management APIs (`/member`) — `MemberResource`

**2 endpoints** managed by `MemberResourceImpl`.

#### 16. POST `/member/link` — Link Member

| Property | Value |
|----------|-------|
| **Description** | Link a member ID to an access ID (SLID) |
| **Request DTO** | `LinkMemberRequest` |
| **Response DTO** | `LinkMemberResponse` |
| **Validator** | `LinkMemberRequestValidator` |
| **Service** | `LinkMemberServiceImpl` → `LinkMemberHelper` |
| **DB Helpers** | `LinkMemberDBHelper` |
| **MQ Event** | `LinkMemberQueueHelper` → publishes member link event |

#### 17. POST `/member/unlink` — Unlink Member

| Property | Value |
|----------|-------|
| **Description** | De-link a member ID from an access ID (SLID) |
| **Request DTO** | `UnlinkMemberIdRequest` |
| **Response DTO** | `CommonResponse` |
| **Validator** | `UnlinkMemberIdRequestValidator` |
| **Service** | `UnlinkMemberIdServiceImpl` → `UnlinkMemberIdHelper` |
| **MQ Event** | `UnlinkMemberIdQueueHelper` → publishes member unlink event |

### Secondary Member ID APIs (`/secmemberid` & `/secmemid`)

#### 18. POST `/secmemberid/add` — Add Secondary Member ID

**Resource:** `AddSecMemberIdResource` → `AddSecMemberIdResourceImpl`

| Property | Value |
|----------|-------|
| **Description** | Add a secondary member ID in the MPSYS system |
| **Request DTO** | `AddSecMemberIdRequest` |
| **Response DTO** | `AddSecMemberIdResponse` |
| **Validator** | `AddSecMemberIdRequestValidator` |
| **Service** | `AddSecMemberIdServiceImpl` → `AddSecMemberIdHelper` |
| **MQ Event** | `AddSecMemberIdQueueHelper` → publishes secondary member ID event |

#### 19. POST `/secmemid/upgrade` — Upgrade Secondary Member ID

**Resource:** `UpgSecMemberIdResource` → `UpgSecMemberIdResourceImpl`

| Property | Value |
|----------|-------|
| **Description** | Upgrade a secondary member ID to primary in the MPSYS system |
| **Request DTO** | `UpgSecMemberIdRequest` |
| **Response DTO** | `UpgSecMemberIdResponse` |
| **Validator** | `UpgSecMemberIdRequestValidator` |
| **Service** | `UpgSecMemberIdServiceImpl` |

### Query Member ID APIs (`/memberid`) — `QueryMemberIdResource`

#### 20. GET `/memberid` — Query Member ID

| Property | Value |
|----------|-------|
| **Description** | Retrieve primary and secondary member ID details |
| **Request** | Headers only (no request body) |
| **Response DTO** | `MemberIdResponse` |
| **Validator** | `QueryMemberIdRequestValidator` |
| **Service** | `QueryMemberIdServiceImpl` → `QueryMemberIdHelper` |
| **DB Helpers** | `MemberIdInfoDbHelper` |

### Contacts APIs (`/contacts`) — `UpdateUserContactCBRResource`

#### 21. POST `/contacts/user/update` — Update User Contact CBR

| Property | Value |
|----------|-------|
| **Description** | Update user contact CBR information |
| **Request DTO** | `UpdateUserContactCBRRequest` |
| **Response DTO** | `UpdateUserContactCBRResponse` |
| **Validator** | `UpdateUserContactCBRRequestvalidator` |
| **Service** | `UpdateUserContactCBRServiceImpl` |
| **DB Helpers** | `ContactDBHelper` |

### Email Alias APIs (`/emailalias`) — `EmailAliasResource`

#### 22. POST `/emailalias` — Remove Email Alias

| Property | Value |
|----------|-------|
| **Description** | Remove email aliases for a user |
| **Request DTO** | `RemoveEmailAliasRequest` |
| **Response DTO** | `CommonResponse` |
| **Validator** | `RemoveEmailAliasRequestValidator` |
| **Service** | `RemoveEmailAliasServiceImpl` → `RemoveEmailAliasHelper` |
| **MQ Event** | `RemoveEmailAliasQueueHelper` → publishes email alias removal event |

### User Deletion APIs — `DeleteUserResource`

#### 23. POST `/deleteuser` — Delete User

| Property | Value |
|----------|-------|
| **Description** | Delete an access ID or member ID when all services linked to these IDs are deleted |
| **Request DTO** | `DeleteUserRequest` |
| **Response DTO** | `DeleteUserResponse` |
| **Validator** | `DeleteUserRequestValidator` |
| **Service** | `DeleteUserServiceImpl` → `DeleteUserHelper` |
| **Helpers** | `DeleteUserCommonHelper`, `DeleteUserUtilHelper` |
| **DB Helpers** | `DeleteMemberDBHelper`, `GetMemberServicesDBHelper` |
| **MQ Event** | `DeleteUserQueueHelper` → publishes user deletion event |

### Profanity Scanner APIs (`/profanity`) — `ProfanityScannerResource`

#### 24. POST `/profanity/scanner` — Profanity Scanner

| Property | Value |
|----------|-------|
| **Description** | Check for bad/profanity words in the given scan item |
| **Request DTO** | `ProfanityScannerRequest` |
| **Response DTO** | `ProfanityScannerResponse` |
| **Validator** | `ProfanityScannerRequestValidator` |
| **Service** | `ProfanityScannerServiceImpl` |
| **Utility** | `BadWordHelper` (loads word lists from `rilhrs-bad-word-data/`) |

### Registration Orchestration APIs (`/orchestration`) — `RegistrationOrchestrationResource`

#### 25. POST `/orchestration/registration` — Registration Orchestration

| Property | Value |
|----------|-------|
| **Description** | Orchestrate registration association processing (AddAssociation) across services for an access ID |
| **Request DTO** | `RegistrationOrchestrationRequest` |
| **Response DTO** | `RegistrationOrchestrationResponse` |
| **Validator** | `RegistrationOrchestrationRequestValidator` |
| **Helper** | `RegistrationOrchestrationHelper` (called directly from the resource) |
| **External Calls** | `RegistrationOrchestrationRestClient` (IDGraphUserAssociationMs AddAssociation) |

### Internal Components (no REST endpoint)

| Property | Value |
|----------|-------|
| **Description** | Internal profile inquiry orchestration (used by other helpers) |
| **Service** | `InquireProfileServiceImpl` → `InquireProfileHelper` |
| **DB Helpers** | `InquireProfileDBHelper`, `MPSInquireProfileDBHelper` |

---

## Request / Response Contracts

### AddUserRequest (Input — `/user/add`)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `userId` | String | Yes | Desired access ID / username |
| `passwordClear` | String | No | Cleartext password (encrypted in transit via Voltage) |
| `browserLanguage` | String | No | Browser locale code |
| `isAccessIdAutoGen` | Boolean | No | Auto-generate access ID if `true` |
| `isTempPassword` | Boolean | No | Mark password as temporary |
| `agentId` | String | No | Agent performing the registration |
| `isExternalCall` | Boolean | No | Whether the call is from an external system |
| `byPassCheckIdAvailability` | Boolean | No | Skip availability check |
| `accessIdRTValidFlag` | Boolean | No | Real-time access ID validation flag |
| `contactEmailRTValidFlag` | Boolean | No | Real-time contact email validation flag |
| `idpTraceId` | String | Yes | Correlation trace ID |
| `cbrNumber` | String | No | CBR number |
| `cbrRTValidFlag` | Boolean | No | Real-time CBR validation flag |
| `contactAddress` | Object | No | Contact address details |

### AddUserResponse (Output — `/user/add`)

| Field | Type | Description |
|-------|------|-------------|
| `appStatusCode` | String | `0` = success |
| `appStatusMsg` | String | Human-readable result message |
| `accessId` | String | Generated or confirmed access ID |
| `guid` | String | Assigned GUID for the new user |
| `idp-trace-id` | String | Correlation trace ID |

### Common Response Pattern

All endpoints return a JSON response with at minimum:

| Field | Type | Description |
|-------|------|-------------|
| `appStatusCode` | String | `0` = success; non-zero = error code |
| `appStatusMsg` | String | Human-readable status message |

### Request DTOs (35 classes in `resource/request/`, incl. nested types)

| DTO | Used By |
|-----|---------|
| `AddUserRequest` | `/user/add` |
| `UpdateUserRequest` | `/user/update` |
| `CheckUserIdAvailabilityRequest` | `/user/checkUserIdAvailability` |
| `ChangeUserNameRequest` | `/user/change` |
| `CheckAccessIdMergeEligibilityRequest` | `/user/checkEligibility` |
| `UpdateUserFraudRequest` | `/user/updateFraud` |
| `ExtUserRequest` | `/user/addExt`, `/user/updateExt` |
| `ValidateUserCredentialsRequest` | `/user/credentials/validate` |
| `MergeUserAccessIdRequest` | `/user/merge` |
| `TransactionRateLimitRequest` | `/register/txnratelimit/verify` |
| `RegisterEmailUserRequest` | `/register/emailuser` |
| `UpdateUserPasswordRequest` | `/userpassword/update` |
| `ResetUserPasswordRequest` | `/userpassword/reset` |
| `UpdatePrepaidUserPasswordRequest` | `/userpassword/updatePrepaid` |
| `LinkMemberRequest` | `/member/link` |
| `UnlinkMemberIdRequest` | `/member/unlink` |
| `AddSecMemberIdRequest` | `/secmemberid/add` |
| `UpgSecMemberIdRequest` | `/secmemid/upgrade` |
| `UpdateUserContactCBRRequest` | `/contacts/user/update` |
| `RemoveEmailAliasRequest` | `/emailalias` |
| `DeleteUserRequest` | `/deleteuser` |
| `ProfanityScannerRequest` | `/profanity/scanner` |
| `RegistrationOrchestrationRequest` | `/orchestration/registration` |
| `InquireProfileRequest` | Internal profile inquiry |
| `BaseUserRequest` | Base class for user requests |

### Response DTOs (40 classes in `resource/response/`, incl. nested types)

| DTO | Used By |
|-----|---------|
| `AddUserResponse` | `/user/add` |
| `UpdateUserResponse` | `/user/update` |
| `CheckUserIdAvailabilityResponse` | `/user/checkUserIdAvailability` |
| `ChangeUserNameResponse` | `/user/change` |
| `CheckAccessIdMergeEligibilityResponse` | `/user/checkEligibility` |
| `UpdateUserFraudResponse` | `/user/updateFraud` |
| `ExtUserResponse` | `/user/addExt`, `/user/updateExt` |
| `ValidateUserCredentialsResponse` | `/user/credentials/validate` |
| `MergeUserAccessIdResponse` | `/user/merge` |
| `TransactionRateLimitResponse` | `/register/txnratelimit/verify` |
| `RegisterEmailUserResponse` | `/register/emailuser` |
| `UpdateUserPasswordResponse` | `/userpassword/update` |
| `ResetUserPasswordResponse` | `/userpassword/reset` |
| `UpdatePrepaidUserPasswordResponse` | `/userpassword/updatePrepaid` |
| `LinkMemberResponse` | `/member/link` |
| `AddSecMemberIdResponse` | `/secmemberid/add` |
| `UpgSecMemberIdResponse` | `/secmemid/upgrade` |
| `MemberIdResponse` | `/memberid` |
| `UpdateUserContactCBRResponse` | `/contacts/user/update` |
| `DeleteUserResponse` | `/deleteuser` |
| `ProfanityScannerResponse` | `/profanity/scanner` |
| `RegistrationOrchestrationResponse` | `/orchestration/registration` |
| `InquireProfileResponse` | Internal profile inquiry |
| `CommonResponse` | `/member/unlink`, `/emailalias` |
| `BaseResponse` | Base class for all responses |

---

## Key Classes and Code References

### Resource Layer (REST Controllers)

| Class | Path | Role |
|-------|------|------|
| `UserProfileResourceImpl` | `resource/impl/` | Add, update, change, merge, validate, fraud, and external user operations |
| `RegisterUserResourceImpl` | `resource/impl/` | Rate-limit verification and email user registration |
| `MemberResourceImpl` | `resource/impl/` | Link and unlink member IDs |
| `UserPasswordResourceImpl` | `resource/impl/` | Password update, reset, and prepaid password update |
| `DeleteUserResourceImpl` | `resource/impl/` | User/member ID deletion |
| `EmailAliasResourceImpl` | `resource/impl/` | Email alias removal |
| `AddSecMemberIdResourceImpl` | `resource/impl/` | Add secondary member ID |
| `UpgSecMemberIdResourceImpl` | `resource/impl/` | Upgrade secondary to primary member ID |
| `ProfanityScannerRosurceImpl` | `resource/impl/` | Profanity word scanning |
| `QueryMemberIdResourceImpl` | `resource/impl/` | Query primary/secondary member IDs |
| `UpdateUserContactCBRResourceImpl` | `resource/impl/` | Update user contact CBR data |
| `RegistrationOrchestrationResourceImpl` | `resource/impl/` | Registration orchestration (AddAssociation processing) |

### Service Layer

| Class | Role |
|-------|------|
| `AddUserServiceImpl` | Orchestrates add-user business logic |
| `UpdateUserServiceImpl` | Orchestrates user profile updates |
| `ChangeUserNameServiceImpl` | Orchestrates username change with eligibility check |
| `DeleteUserServiceImpl` | Orchestrates user/member deletion |
| `LinkMemberServiceImpl` / `UnlinkMemberIdServiceImpl` | Member ID linking/unlinking |
| `MergeUserAccessIdServiceImpl` | Access ID merge via SOAP + DB operations |
| `AddSecMemberIdServiceImpl` / `UpgSecMemberIdServiceImpl` | Secondary member ID add/upgrade |
| `RegisterEmailUserServiceImpl` | OPR email user registration |
| `ResetUserPasswordServiceImpl` / `UpdateUserPasswordServiceImpl` | Password operations |
| `UpdatePrepaidUserPasswordServiceImpl` | Prepaid password update |
| `CheckUserIdAvailabilityServiceImpl` | Username availability check |
| `CheckAccessIdMergeEligibilityServiceImpl` | Merge eligibility verification |
| `TxnRateLimitStatusServiceImpl` | IP-based rate limit status |
| `UpdateUserFraudServiceImpl` | Fraud lock updates |
| `ValidateUserCredentialsServiceImpl` | Credential validation |
| `ProfanityScannerServiceImpl` | Bad word scanning |
| `QueryMemberIdServiceImpl` | Member ID querying |
| `AddExtUserServiceImpl` / `UpdateExtUserServiceImpl` | External user CRUD |
| `RemoveEmailAliasServiceImpl` | Email alias removal |
| `InquireProfileServiceImpl` | Profile inquiry orchestration |
| `UpdateUserContactCBRServiceImpl` | Contact CBR update |

### Helper Layer

| Class | Role |
|-------|------|
| `AddUserHelper` | Core add-user DB interaction and event publishing |
| `ChangeUserNameHelper` | Username change logic with merge eligibility |
| `DeleteUserHelper` / `DeleteUserCommonHelper` / `DeleteUserUtilHelper` | Multi-step deletion logic |
| `LinkMemberHelper` / `UnlinkMemberIdHelper` | Member link/unlink DB operations |
| `MergeUserAccessIdHelper` | Access ID merge logic |
| `MergeSlidProfileSoapHelper` | SOAP client for MPS MergeSLIDProfile |
| `CheckUserIdAvailabilityHelper` | Availability check logic |
| `CheckAccessIdMergeEligibilityHelper` | Merge eligibility rules |
| `CheckAgeEligibilityHelper` | Age eligibility verification |
| `CTNEligibilityHelper` | CTN eligibility verification |
| `RegisterEmailUserHelper` | Email user registration logic |
| `RemoveEmailAliasHelper` | Email alias removal logic |
| `AddSecMemberIdHelper` | Secondary member ID addition |
| `TxnRateLimitStatusHelper` / `IPTxnRateLimitHelper` | Rate limit logic |
| `InquireProfileHelper` | Profile inquiry delegation |
| `AccountLockHelper` | Account lockout management |
| `HandleGenerationAlgorithm` | Handle/ID generation |
| `QueryMemberIdHelper` | Member ID query logic |
| `AddExtUserHelper` / `UpdateExtUserHelper` | External user helper operations |
| `AbstractHelper` | Base helper with shared utilities |

### Request Validators (24 total)

Every endpoint has a dedicated validator in `request/validator/`:

| Class | Validates |
|-------|-----------|
| `AddUserRequestValidator` | `/user/add` |
| `UpdateUserRequestValidator` | `/user/update` |
| `CheckUserIdAvailabilityRequestValidator` | `/user/checkUserIdAvailability` |
| `ChangeUserNameRequestValidator` | `/user/change` |
| `CheckAccessIdMergeEligibilityRequestValidator` | `/user/checkEligibility` |
| `UpdateUserFraudRequestValidator` | `/user/updateFraud` |
| `ExtUserRequestValidator` | `/user/addExt`, `/user/updateExt` |
| `ValidateUserCredentialsRequestValidator` | `/user/credentials/validate` |
| `MergeUserAccessIdRequestValidator` | `/user/merge` |
| `TxnRateLimitStatusRequestValidator` | `/register/txnratelimit/verify` |
| `RegisterEmailUserRequestValidator` | `/register/emailuser` |
| `UpdateUserPasswordRequestValidator` | `/userpassword/update` |
| `ResetUserPasswordRequestValidator` | `/userpassword/reset` |
| `UpdatePrepaidUserPasswordRequestValidator` | `/userpassword/updatePrepaid` |
| `LinkMemberRequestValidator` | `/member/link` |
| `UnlinkMemberIdRequestValidator` | `/member/unlink` |
| `AddSecMemberIdRequestValidator` | `/secmemberid/add` |
| `UpgSecMemberIdRequestValidator` | `/secmemid/upgrade` |
| `QueryMemberIdRequestValidator` | `/memberid` |
| `UpdateUserContactCBRRequestvalidator` | `/contacts/user/update` |
| `RemoveEmailAliasRequestValidator` | `/emailalias` |
| `DeleteUserRequestValidator` | `/deleteuser` |
| `ProfanityScannerRequestValidator` | `/profanity/scanner` |
| `RegistrationOrchestrationRequestValidator` | `/orchestration/registration` |

### Configuration Classes

| Class | Role |
|-------|------|
| `Application` | Spring Boot entry point (`@SpringBootApplication`, `@EnableAsync`, `@EnableRetry`, `@EnableCaching`) |
| `JerseyConfiguration` | Registers all 12 JAX-RS resource implementations and exception mappers |
| `DataBaseConfiguration` | Oracle XA datasource, Hibernate/JPA entity manager, dynamic config refresh |
| `JMSConfiguration` | IBM MQ connection factory, caching connection factory, JmsTemplate |
| `MPSSOAPConfig` | SOAP HTTP client for MPS MergeSLIDProfile with proxy support |
| `MPSYSConfiguration` | MPS system integration configuration |
| `BeanConfiguration` | General bean definitions |
| `CacheConfiguration` | Spring caching setup |
| `CommonConfig` | Shared configuration values |
| `CommonHelperBeanConfiguration` | Common helper bean registration |
| `StaticDataLoaderConfig` | Static/reference data loading at startup |
| `TransactionConfiguration` | JPA transaction management |
| `ManagedShutdownConfiguration` | Graceful shutdown with Jersey filters |
| `IDGraphAzureAppConfig` | Azure App Configuration integration (dynamic config refresh) |
| `ScheduledTaskConfiguration` / `SchedulerConfig` | Scheduled task setup |
| `ConfigChangeEventListener` / `OnConfigChange` | Config change event handling |

### Utility Classes (10 classes in `utils/`)

| Class | Role |
|-------|------|
| `BadWordHelper` | Profanity word list loading and scanning |
| `CommonUtilHelper` | Shared utility methods |
| `SharedUtilHelper` | Cross-cutting helper utilities |
| `InquireProfileUtilHelper` | Profile inquiry utility methods |
| `RILCheckAndEncryptPasswordHelper` | Password encryption via RIL check |
| `DateUtil` | Date formatting and parsing |
| `JsonService` | JSON serialization/deserialization |
| `AppCategoryCodeProvider` | Application category code resolution |
| `LogInformationConstants` | Logging constant definitions |
| `ProfileOrchestrationConstants` | Profile orchestration constant values |

### Exception Handling (5 classes in `exceptions/`)

| Class | Role |
|-------|------|
| `MPSException` | Core service exception type |
| `MPSExceptionMapper` | Maps `MPSException` to JAX-RS error responses |
| `InvalidInputDataException` | Input validation exception |
| `InvalidInputDataExceptionMapper` | Maps validation exceptions to 400 responses |
| `ValidationExceptionMapper` | Maps Jakarta validation exceptions to error responses |

### Health Check (2 classes in `healthcheck/`)

| Class | Role |
|-------|------|
| `DatabaseHealthIndicator` | Oracle DB connectivity health check |
| `JMHealthCheckIndicator` | JMS/IBM MQ connectivity health check |

### Message Classes (4 classes in `message/`)

| Class | Role |
|-------|------|
| `ErrorMessages` | Centralized error message constants |
| `LogMessages` | Centralized log message constants |
| `ClientExceptionMessage` | Client exception message formatting |
| `SoapClient` | SOAP client message utilities |

### Source Code Reference Paths

```
src/main/java/com/att/idp/idgraphusermanagementms/
├── Application.java                        # Spring Boot entry point
├── appconfig/config/                       # Azure App Configuration (5 classes)
├── client/                                 # SOAP handler (MPSMergeSLIDProfileSoapHandler)
├── config/                                 # All Spring @Configuration classes (13 classes)
├── db/helper/                              # Database helper classes (35 classes)
├── db/model/                               # JPA entity models
├── db/repository/                          # Spring Data repositories
├── db/request/                             # DB request objects
├── db/response/                            # DB response objects
├── exceptions/                             # Exception mappers (5 classes)
├── healthcheck/                            # Health check indicators (2 classes)
├── helper/                                 # Business logic helpers (34 top-level classes)
│   ├── resetuserpassword/                  # Password reset helpers
│   └── voltage/                            # Voltage encryption helpers
├── integration/                            # MPSYSInternalRestClient, RegistrationOrchestrationRestClient (MS-to-MS calls)
├── message/                                # Message constants and DTOs (4 classes)
├── model/                                  # Domain models (ErrorMessage)
├── queue/message/helpers/                  # MQ queue message builders (12 classes)
├── queue/message/sender/                   # JMSQueueSender
├── request/validator/                      # Request validators (24 classes)
├── resource/                               # JAX-RS resource interfaces (12 interfaces)
├── resource/impl/                          # JAX-RS resource implementations (12 classes)
├── resource/request/                       # Request DTOs (35 classes incl. nested types)
├── resource/response/                      # Response DTOs (40 classes incl. nested types)
├── service/                                # Service interfaces (25 interfaces)
├── service/impl/                           # Service implementations (25 classes)
└── utils/                                  # Utilities (10 classes)
```

---

## Data Stores & State Management

| Store / State | Type | Purpose | Access Pattern | Notes |
|---------------|------|---------|----------------|-------|
| Oracle DB | DB | User, member, service, contact, fraud, rate-limit data | JPA/Hibernate via 35 DB Helpers (XA DataSource) | Dialect: `OracleDialect`; DDL auto: `none`; batch size: 30 |
| IBM MQ Queues | Messaging | Async lifecycle event publishing to DBUS | `JMSQueueSender` (hash-based routing) | Failover to secondary MQ host; 3-attempt retry |
| Azure Event Hub | Event Stream | External event notifications (user lifecycle) | Spring Cloud Stream Kafka binder (dual primary/secondary) | Topic: `*-idgraph-external-events-v1` via `idgrapheventshelper` |
| Kafka (Audit) | Event Stream | Audit watermarking and event tracking | Spring Kafka producer | Topics: `com.att.idp.audit.watermarking.*`, `com.att.idp.audit.tracking.*` |
| Azure App Configuration | Config State | Dynamic runtime config for DataSource pool | `IDGraphAzureAppConfig` + `ConfigChangeEventListener` | Scheduled refresh |
| Voltage Key Cache | Encryption State | Cached Voltage FPE/FFX encryption keys | `voltageKeyCache/` at runtime | Keys loaded at startup |

The service connects to an **Oracle database** using JPA/Hibernate via Tomcat XA DataSource.

**Key DB Helpers (35 classes in `db/helper/`):**

| Class | Purpose |
|-------|---------|
| `AbstractDBHelper` | Base class with shared DB utilities |
| `DBHelper` | General-purpose DB operations |
| `AddMemberDBHelper` | Insert member records |
| `AddServiceDBHelper` | Add service records |
| `DeleteMemberDBHelper` | Remove member records |
| `AccountLockoutDBHelper` | Account lockout read/write |
| `ContactDBHelper` | Contact information CRUD |
| `CTNEligibilityDBHelper` | CTN eligibility lookups |
| `EmailAppPasswordDBHelper` | Email app password operations |
| `FraudLockDBHelper` | Fraud lock read/write |
| `GetAllInfoDBHelper` | Retrieve all user info |
| `GetAssociatedMembersDBHelper` | Query associated member IDs |
| `GetInfoByMaskDBHelper` | Masked data retrieval |
| `GetMemberRolesDBHelper` | Member role lookups |
| `GetMemberServicesDBHelper` | Member service associations |
| `GetProfileByGroupNumDBHelper` | Profile lookup by group number |
| `GetSlidInfoDBHelper` | SLID information retrieval |
| `HealthCheckDBHelper` | Database health check queries |
| `IPRateLimitDBHelper` | IP rate limit data |
| `InquireProfileDBHelper` | Profile inquiry queries |
| `LinkMemberDBHelper` | Member-to-access-ID linking |
| `MPSInquireProfileDBHelper` | MPS profile inquiry |
| `MemberDBHelper` | Core member operations |
| `MemberFraudPasswordDBHelper` | Fraud/password member data |
| `MemberServiceAttributeDBHelper` | Member service attributes |
| `MemberIdInfoDbHelper` | Member ID info queries |

**Hibernate properties:**
- Dialect: `org.hibernate.dialect.OracleDialect`
- Batch size: 30
- Ordered inserts/updates: enabled
- DDL auto: `none` (schema managed externally)

---

## Async Messaging (JMS / IBM MQ)

The service publishes lifecycle events to **IBM MQ** queues for downstream processing via DBUS.

**Components:**

| Component | Role |
|-----------|------|
| `JMSConfiguration` | Configures MQ connection factory with failover, caching, and reconnect |
| `JMSQueueSender` | Sends serialized HashMap messages to MQ queues with hash-based routing |
| `PublishMqAehHelper` | Coordinates MQ + Azure Event Hub publishing |

**Queue message helpers** (each prepares event data for a specific operation):

| Helper | Event |
|--------|-------|
| `AddUserQueueHelper` | User registration events |
| `ChangeUserNameQueueHelper` | Username change events |
| `DeleteUserQueueHelper` | User deletion events |
| `LinkMemberQueueHelper` | Member link events |
| `UnlinkMemberIdQueueHelper` | Member unlink events |
| `UpdateUserQueueHelper` | User update events |
| `UpdateUserFraudQueueHelper` | Fraud update events |
| `PasswordQueueHelper` | Password change events |
| `RegisterEmailUserQueueHelper` | Email user registration events |
| `RemoveEmailAliasQueueHelper` | Email alias removal events |
| `AddSecMemberIdQueueHelper` | Secondary member ID events |

**MQ behavior:**
- Hash-based queue routing using `mainHashKey` to distribute across multiple queue instances
- Automatic failover to a secondary MQ host on connection failure
- Retry logic (3 attempts) with thread-safe publish status tracking
- Messages serialized as Java byte arrays with JMS correlation ID

---

## External Integrations

### MS-to-MS REST Calls (`MPSYSInternalRestClient`)

The service makes internal REST calls to sibling IDGraph microservices:

| Transaction Name | Target Service | Endpoint Config Property |
|-----------------|----------------|--------------------------|
| `CheckUserIdAvailability` | IDGraphUserManagementMs (OUD) | `apiclient.rest.idgoud.cuia.endpoint` |
| `AddUser` | IDGraphUserManagementMs (OUD) | `apiclient.rest.idgoud.au.endpoint` |
| `AddAssociation` | IDGraphUserAssociationMs (OUA) | `apiclient.rest.idgoua.aa.endpoint` |
| `RemoveAssociation` | IDGraphUserAssociationMs (OUA) | `apiclient.rest.idgoua.ra.endpoint` |
| `ChangeStatus` | IDGraphUserAssociationMs (OUA) | `apiclient.rest.idgoua.cs.endpoint` |
| `InquireProfile` | IDGraphUserProfileMs (IUP) | `apiclient.rest.idgiup.ip.endpoint` |
| `InquireAssociatedAccessIds` | IDGraphUserProfileMs (IUP) | `apiclient.rest.idgiup.iaa.endpoint` |
| `OrderGraph` | OrderGraph (OG) | `apiclient.rest.ogs.og.endpoint` |
| `AddAssociation` (orchestration) | IDGraphUserAssociationMs (OUA) | `apiclient.rest.idgoua.aa.endpoint` via `RegistrationOrchestrationRestClient` |

All calls use Basic authentication, JSON payloads, and include timeout/retry handling.

### SOAP Integration (MergeSLIDProfile)

- **WSDL**: `src/main/resources/schemas/mpsys/mergeSLIDProfile/RILMergeSLIDProfile.wsdl`
- **JAXB bindings**: Auto-generated from XSD/WSDL at build time via `jaxb-maven-plugin`
- **Handler**: `MPSMergeSLIDProfileSoapHandler` manages SOAP envelope customization
- **Config**: `MPSSOAPConfig` configures `HttpComponentsMessageSender` with optional Azure proxy support

---

## Configuration

### Secrets Management

Secrets are loaded at startup from Azure Key Vault via `IdpSecretsCacheHelper` as defined in `idpsecretsloader.yaml`:

| Secret | Purpose |
|--------|---------|
| `aaf-id` / `aaf-password` | AAF authentication |
| `idgraph-spring-datasource-password` | Oracle DB password (AES-256 encrypted) |
| `idgraph-aes256key` | AES-256 encryption key |
| `idgraph-mps-soap-authorization` | MPS SOAP Basic Auth credentials |
| `idgraph-lscrm-*` | LSCRM integration credentials |
| `idgraph-bds-password` / `idgraph-bls-enc-md5-key` | BDS/BLS credentials |
| `global_voltage_*` | Voltage encryption credentials |

### Azure App Configuration

Dynamic configuration refresh is supported via `IDGraphAzureAppConfig`:
- DataSource pool parameters (connections, timeouts, validation queries) can be refreshed at runtime
- `ConfigChangeEventListener` and `OnConfigChange` handle refresh events
- `ScheduledTaskConfiguration` schedules periodic config refresh checks

### Key Application Properties

| Property | Description |
|----------|-------------|
| `spring.datasource.url` | Oracle JDBC connection URL |
| `spring.datasource.driver-class-name` | Oracle JDBC driver |
| `mq.host` / `mq.port` / `mq.channel` | Primary IBM MQ connection |
| `mq.ff.host` / `mq.ff.port` / `mq.ff.channel` | Failover IBM MQ connection |
| `mq.queue.name` / `mq.queue.count` | MQ queue name pattern and hash count |
| `apiclient.rest.idgoud.url` | OUD (User Management) base URL |
| `apiclient.rest.idgoua.url` | OUA (User Association) base URL |
| `apiclient.rest.idgiup.url` | IUP (User Profile) base URL |
| `apiclient.rest.og.url` | OrderGraph base URL |
| `enable.http.proxy` | Enable Azure proxy for SOAP calls |

---

## Dependencies

### Core Framework

| Library | Version | Purpose |
|---------|---------|---------|
| Spring Boot | 3.x (Seed SDK 3.0.1) | Application framework |
| JAX-RS / Jersey | (Seed managed) | REST API layer |
| Spring Data JPA / Hibernate | (Seed managed) | ORM and database access |
| Spring JMS | (Seed managed) | JMS messaging abstraction |
| Spring Cloud Azure App Configuration | 5.23.0 | Dynamic config from Azure App Configuration |
| Spring Cloud | 2025.0.0 | Cloud-native features |

### Database & Messaging

| Library | Version | Purpose |
|---------|---------|---------|
| Oracle JDBC (ojdbc8) | 19.8.0.0 | Oracle database connectivity |
| Tomcat JDBC Pool | 9.0.65 | XA DataSource connection pooling |
| HikariCP | (Seed managed) | Additional connection pooling |
| IBM MQ Jakarta Client | 9.4.2.1 | IBM MQ JMS messaging |

### API & Documentation

| Library | Version | Purpose |
|---------|---------|---------|
| Swagger Core v3 (Jakarta) | (Seed managed) | OpenAPI 3 annotations and spec generation |
| Swagger Jersey2 JAX-RS | 1.6.12 | Swagger integration with Jersey |
| SpringDoc OpenAPI | (Seed managed) | Swagger UI (`/swagger-ui.html`) |

### Security & Encryption

| Library | Version | Purpose |
|---------|---------|---------|
| `idp-voltage-encrypt` / `idp-voltage-decrypt` | (Seed managed) | Voltage FPE/FFX encryption/decryption |
| `idp-aaf` | (Seed managed) | AAF authentication framework |
| `idp-config` | (Seed managed) | Secrets and configuration management |

### Internal Libraries

| Library | Version | Purpose |
|---------|---------|---------|
| `IDGraphCommonHelper` | 3.0.3 | Shared IDGraph utility classes (`CommonDefs`, `CErrorDefs`, `CommonErrorMap`) |
| `IDGraphStaticHelper` | 0.0.1-SNAPSHOT | Static/reference data utilities |
| `idgrapheventshelper` | 4.0.2 | Event publishing helpers (Azure Event Hub) |
| `idgraphoracledbhelper` | 3.0.1 | Shared Oracle DB helper utilities |
| `CGClientHelpers` | 3.0.10 | CG client integration helpers |
| `rest-api-client` | (Seed managed) | REST client with `RestTemplateBeanFactory` |
| `soap-api-client` | (Seed managed) | SOAP client framework |
| `feature-toggle-core` | 2.8.0 | Feature flag management |

### Build & Test

| Library | Version | Purpose |
|---------|---------|---------|
| Lombok | 1.18.32 | Boilerplate reduction (`@Data`, `@Builder`, etc.) |
| Jackson | (Seed managed) | JSON serialization/deserialization |
| OkHttp3 | 4.12.0 | HTTP client |
| Spock Framework | 2.3-groovy-4.0 | BDD-style testing (Groovy) |
| Apache Groovy | 4.0.21 | Groovy language for Spock tests |
| TestContainers (Azure, Spock) | 1.20.x | Integration test containers |
| MockServer | 5.11.2 | HTTP mock server for integration tests |
| JUnit Platform Suite | (Seed managed) | Test suite organization |
| JaCoCo | (Seed managed) | Code coverage |

---

## Build and Test

### Build

```bash
# Full build with unit tests
mvn clean install

# Skip tests
mvn clean install -DskipTests

# Run locally
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

### Run Tests

```bash
# Unit tests (default)
mvn clean test

# Integration tests
mvn clean verify -P integration-test

# Test summary script
./sum_junit_tests.sh
```

### Test Organization

Tests are organized in three directories:

| Directory | Framework | Purpose |
|-----------|-----------|---------|
| `src/test/java/` | JUnit 5 | Unit tests |
| `src/test/groovy/` | Spock (Groovy) | BDD-style unit and component tests |
| `src/test/ist/` | Integration | Integration/IST test data and configs |

**Maven test profiles:**

| Profile | Unit Tests | Integration Tests |
|---------|------------|-------------------|
| `local` (default) | Enabled | Skipped |
| `integration-test` | Skipped | Enabled |

**Coverage exclusions** (via JaCoCo):
- `Application`, config classes, exception classes, model/DTO classes
- Voltage encryption/decryption utilities
- Health check classes
- Select helper classes (e.g., `MergeUserAccessIdHelper`, `QueryProfileProofingInfoHelper`)

---

## Deployment

### Build Artifact / Image

```bash
# Docker image built via docker-maven-plugin (Spotify)
mvn clean package -DskipTests
# Image published to internal registry as com.att.idp/idgraphusermanagementms
```

### Pipeline

- **Build Server**: https://jenkins-community-gateway.az.3pc.att.com/idp-ui/jenkins/job/com.att.idp/job/com.att.idp.IDGraphUserManagementMs/job/master
- **Pipeline URL**: https://pipeline.web.att.com/uui
- **Build system:** Jenkins (using `idp-shared-library@release/3.0.0`) plus GitHub Actions CI (`.github/workflows/ci.yaml` → reusable workflow `apm0015724-platdevops-reusable-workflows/java-svc-ci.yaml@v1-java-svc`, adopted 2026-07)

| Parameter | Value |
|-----------|-------|
| Veracode Profile | `33944-IDGraph-IDGraphUserManagementMs` |
| Default Cluster | `Graph1EUS2-dev2` |
| Default Namespace | `com-att-idp-idgraph-dev2` |
| Skipped Quality Gates | `QG4.1`, `QG4.2`, `QG7` |
| GitHub Migrated | `true` |

**Docker image:** Built via `docker-maven-plugin` (Spotify), published to internal registry as `com.att.idp/idgraphusermanagementms`.

**Pipeline URL:** https://pipeline.web.att.com/uui

**Build Server:** https://jenkins-community-gateway.az.3pc.att.com/idp-ui/jenkins/job/com.att.idp/job/com.att.idp.IDGraphUserManagementMs/job/master

**Related deployment repos:**

| Repository | Purpose |
|------------|---------|
| `apm0045194-idgraph-idgraphusermanagementms-helm` | Helm charts and per-environment values |
| `apm0045194-idgraph-idgraphusermanagementms_configrole` | Ansible config role templates |
| `apm0045194-idgraph-idgraphusermanagementms_playbook` | Ansible deployment playbooks |

---

## Workflows

### Application Startup Sequence

1. `IdpSecretsCacheHelper.init()` loads secrets from Azure Key Vault
2. `SystemPropertiesLoader.addSystemProperties()` sets environment-specific system properties
3. Spring Boot initializes with `@EnableAsync`, `@EnableRetry`, `@EnableCaching`
4. `DataBaseConfiguration` creates Oracle XA DataSource and JPA EntityManager
5. `JMSConfiguration` establishes IBM MQ connection (with failover test)
6. `StaticDataLoaderConfig` loads reference data
7. `JerseyConfiguration` registers all 12 resource implementations and exception mappers
8. REST endpoints available at `/idgraphusermanagementms/v1/`

### Adding a New Endpoint

1. Define the method signature in a `Resource` interface (or create a new one) with JAX-RS + OpenAPI 3 annotations
2. Implement the method in the corresponding `ResourceImpl` class
3. Create a `RequestValidator` in `request/validator/` for header and body validation
4. Create a `Service` interface and `ServiceImpl` in `service/` and `service/impl/`
5. Create a `Helper` class for core business logic and DB interaction
6. If async events are needed, create a `QueueHelper` in `queue/message/helpers/`
7. Register the new `ResourceImpl` in `JerseyConfiguration`
8. Add Spock/JUnit tests for the validator, service, and helper
9. Update Sonar/JaCoCo exclusions in `pom.xml` if needed

### Building Locally

```bash
# Full build with unit tests
mvn clean install

# Skip tests
mvn clean install -DskipTests

# Run with local profile
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

---

## Observability and Operations

| Area | Mechanism | Notes |
|------|-----------|-------|
| **Logging** | `idp-logging-core 2.9.101` via `logback.xml` | Structured logs; ESAPI-sanitized external values; masking via `masking.properties` |
| **Metrics** | Micrometer histograms per endpoint | `addUser`, `updateUser`, `changeUserName`, `deleteUser`, `linkMember`, `mergeUserAccessId` latencies — 75th/90th/95th/99th percentiles |
| **Tracing / Correlation** | `idp-trace-id` header | Propagated on all inbound requests and passed to downstream calls |
| **Health Checks** | `DatabaseHealthIndicator`, `JMHealthCheckIndicator` | Spring Boot Actuator health endpoints for Oracle DB + IBM MQ |
| **Config Refresh** | `ScheduledTaskConfiguration` + `ConfigChangeEventListener` | Periodic Azure App Configuration polling for DataSource pool params |
| **Audit** | `audit4j.conf.yml` | Request/response audit logging |

---

## Security and Compliance

| Area | Approach | Notes |
|------|----------|-------|
| **Authentication / Authorization** | AAF (`idp-aaf`) + Entra ID OAuth2 | Role-based via `AAFUserRoles.properties`; Entra RBAC rules in `entra-authz-rules.yaml` (`Application.Read` for GET, `Application.Write` for POST) when `idp.api-auth.entra.oauth.enabled` |
| **Sensitive Data Handling** | Voltage FPE/FFX encryption + ESAPI log sanitization | Passwords encrypted via `RILCheckAndEncryptPasswordHelper`; no PII in logs |
| **Secrets Management** | Azure Key Vault via `idpsecretsloader.yaml` | DB password, AES-256 key, SOAP auth, Voltage credentials — loaded before Spring context |
| **Input Validation** | 24 dedicated `RequestValidator` classes | Header + body validation on every endpoint; `InvalidInputDataException` on failure |
| **Security Config** | `security-filter-config.yaml` + `cadi.properties` | AAF CADI filter configuration |
| **Compliance Considerations** | PII masking, Voltage encryption for password data | `masking.properties` controls field masking in logs |

---

## Change Log

| Version | Date | Description |
|---------|------|-------------|
| 4.0.4 | 2026-08-03 | Deep-scan refresh — documented `POST /orchestration/registration` (Registration Orchestration, added 2026-05, replaces stale internal `/inquireProfile` endpoint row), updated pom version to `0.0.2-SNAPSHOT`, corrected class counts (12 resources, 24 validators, 25 services, 34 helpers, 35 DB helpers), added `RegistrationOrchestrationRestClient` integration, Entra ID OAuth2 authz rules, and GitHub Actions `v1-java-svc` reusable CI workflow (adopted 2026-07); recent functional changes: fraud-lock upsert (SPTAUTHREG-37319), reset-password fixes (SPTAUTHREG-37530), MergeUserAccessId/LinkMember hardening (SPTAUTHREG-35932/36983), QueryMemberId ISAAC enhancements + `isLegacy` field (SPTAUTHREG-36586), CWE-117 log-injection fix (SPTAUTHREG-37516), input-validation denylist restore (CDEX-537703), external events broker alignment (SPTAUTHREG-37294) |
| 4.0.3 | 2026-05-01 | /omni-document --readme refresh — Fixed Component Type (added Kafka + SOAP), corrected base path to `/msapi/idgraphusermanagementms/v1`, corrected Swagger UI path, added Azure Event Hub + Kafka Audit + Voltage + CustomerGraph to architecture diagram and integration context, added Event Hub and Kafka Audit to Data Stores |
| 4.0.2 | 2026-04-30 | /omni-document --readme refresh — Fixed IDGraphCommonHelper version (3.0.2→3.0.3), CGClientHelpers version (3.0.1→3.0.10), removed duplicate test commands section |
| 4.0.1 | 2026-04-28 | /omni-document --readme refresh — Table of Contents expanded to include all document sections (Request/Response Contracts, Key Classes and Code References, Async Messaging, External Integrations, Workflows); frontmatter date updated |
| 4.0.0 | 2026-04-27 | OMNI template compliance — added YAML frontmatter, metadata table, Mermaid architecture diagram, Project Structure, Domain Capability Map, Integration Context, Data Stores & State Management, Observability and Operations, Security and Compliance, Build and Test, and Deployment sections |
| 3.0.0 | 2026-03-20 | Complete README rewrite — API-first documentation with all 25 endpoints detailed individually, full request/response DTO catalog, complete validator listing (23/23), utility/exception/healthcheck/message class documentation, expanded source code tree |
| 2.0.0 | 2025-02-28 | Added process flow diagrams, architecture diagram, inputs/outputs section, metrics glossary |
| 1.0.0 | — | Initial README with basic overview |

---

This repo has been upgraded using Seed v3.0.0 upgrade automation.
