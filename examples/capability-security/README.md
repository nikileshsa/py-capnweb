# Capability Security Examples

This directory contains examples demonstrating the **Object Capability Security Model** 
implemented in Cap'n Web. Each example showcases a key security property.

## Key Security Properties

| Property | Example | Description |
|----------|---------|-------------|
| **Attenuation** | `bank_account/` | Derived capabilities have reduced permissions |
| **Revocation** | `revocable_access/` | Capabilities can be invalidated at any time |
| **Confinement** | `secure_storage/` | Capabilities cannot be forged or guessed |
| **POLA** | `least_authority/` | Only grant the minimum needed capabilities |
| **Isolation** | `multi_tenant/` | Tenants are completely isolated from each other |

## Running the Examples

Each example has a `server.py` and `client.py`. Run the server first:

```bash
# Terminal 1: Start the server
python examples/capability-security/bank_account/server.py

# Terminal 2: Run the client
python examples/capability-security/bank_account/client.py
```

## Security Model Overview

### What is Object Capability Security?

In the object capability model:

1. **No Ambient Authority** - Code cannot access resources just because they exist.
   Access requires an explicit capability (object reference).

2. **Capabilities are Unforgeable** - You cannot create a capability out of thin air.
   You must receive it from someone who already has it.

3. **Capabilities can be Attenuated** - When passing a capability, you can wrap it
   to provide reduced permissions.

4. **Capabilities can be Revoked** - The grantor can invalidate a capability at any time.

### How Cap'n Web Implements This

- **RpcTarget** - Objects that can be accessed remotely. Each RpcTarget is a capability.
- **RpcStub** - A reference to a remote capability. Cannot be forged.
- **Attenuation** - Create wrapper RpcTargets that restrict operations.
- **Revocation** - Track capability state and reject operations when revoked.

## Example Scenarios

### 1. Bank Account (Attenuation)

Shows how a full bank account capability can be attenuated to:
- Read-only view (can only check balance)
- Limited withdrawal (can only withdraw up to a limit)

### 2. Revocable Access (Revocation)

Demonstrates how access tokens can be:
- Granted with specific permissions
- Revoked at any time by the grantor
- Independently managed (revoking one doesn't affect others)

### 3. Secure Storage (Confinement)

Shows that:
- Resources can only be accessed via their capability
- There's no way to enumerate or guess resource IDs
- Capabilities are scoped to specific resources

### 4. Least Authority (POLA)

Demonstrates the Powerbox pattern:
- Users request capabilities through a trusted intermediary
- Only the minimum required permissions are granted
- All grants are logged for auditing

### 5. Multi-Tenant (Isolation)

Shows complete tenant isolation:
- Each tenant gets their own capability
- No way to access another tenant's data
- Even knowing tenant IDs doesn't help without the capability
