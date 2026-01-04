# Capability Security Examples

This directory contains examples demonstrating the **Object Capability Security Model** 
implemented in Cap'n Web.

## Bank Account Example (Capability Attenuation)

Demonstrates how capabilities can be **attenuated** (reduced) to provide limited access:
- **Full Account** - Can deposit, withdraw, check balance, freeze
- **Read-Only View** - Can only check balance (deposit/withdraw blocked)
- **Limited Withdrawal** - Can withdraw up to a limit, then blocked

### Running the Example

**Step 1: Start the server**

```bash
cd capnweb-python
uv run python examples/capability-security/bank_account/server.py
```

**Expected output:**
```
INFO:__main__:============================================================
INFO:__main__:BANK SERVER - Capability Attenuation Demo
INFO:__main__:============================================================
INFO:__main__:
INFO:__main__:Security Properties Demonstrated:
INFO:__main__:  - ATTENUATION: Derived caps have fewer permissions
INFO:__main__:  - POLA: Grant only needed permissions
INFO:__main__:  - REVOCATION: Freeze accounts to revoke access
INFO:__main__:
INFO:__main__:Endpoint: ws://127.0.0.1:8080/rpc/ws
INFO:__main__:
INFO:__main__:Run client: python examples/capability-security/bank_account/client.py
```

**Step 2: Run the client (in a new terminal)**

```bash
cd capnweb-python
uv run python examples/capability-security/bank_account/client.py
```

**Expected output:**
```
Created account
Initial balance: $1000.0
After deposit: $1500.0
Read-only view balance: $1500.0
Read-only deposit blocked: permission_denied
Limited withdrawal: $50.0, remaining limit: $150.0
Limit exceeded blocked: permission_denied
```

### What the Example Demonstrates

1. **Create Account** - Full capability with all permissions
2. **Deposit/Withdraw** - Normal operations work
3. **Create Read-Only View** - Attenuated capability that can only read
4. **Read-Only Deposit Blocked** - `permission_denied` error
5. **Create Limited Withdrawal** - Attenuated capability with $200 limit
6. **Exceed Limit Blocked** - `permission_denied` when limit exceeded

## Key Security Properties

| Property | Description |
|----------|-------------|
| **Attenuation** | Derived capabilities have reduced permissions |
| **POLA** | Only grant the minimum needed permissions |
| **Revocation** | Freeze accounts to revoke all access |

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
