# WebTransport Example

> ⚠️ **Known Issue**: The WebTransport RPC layer currently has a timeout bug. The connection establishes successfully but RPC calls may timeout. This is being investigated.

This example demonstrates using WebTransport/HTTP/3 for high-performance RPC communication with Cap'n Web.

## Features

- **HTTP/3/QUIC**: Modern transport protocol with built-in multiplexing
- **Zero-RTT reconnection**: Fast reconnection after initial handshake
- **Better performance**: Lower latency than WebSocket, especially over lossy networks
- **TLS 1.3**: Built-in encryption and security

## Prerequisites

WebTransport requires aioquic library:

```bash
uv pip install aioquic
# or
pip install aioquic
```

## Files

- **`server.py`** - WebTransport server with calculator RPC target
- **`client.py`** - WebTransport client demonstrating RPC calls
- **`generate_certs.py`** - Helper script to generate self-signed certificates

## Running the Example

### Step 1: Generate Certificates

WebTransport requires TLS certificates. For development, use self-signed certificates:

```bash
python examples/webtransport/generate_certs.py
```

This creates:
- `localhost.crt` - SSL certificate
- `localhost.key` - Private key

### Step 2: Start the Server

In one terminal:

```bash
python examples/webtransport/server.py
```

Output:
```
INFO:capnweb.webtransport:Starting WebTransport server on localhost:4433
Server listening on https://localhost:4433/rpc/wt
Press Ctrl+C to stop
```

### Step 3: Run the Client

In another terminal:

```bash
python examples/webtransport/client.py
```

Output:
```
--- WebTransport Calculator Demo ---
Connecting to https://localhost:4433/rpc/wt...
INFO:capnweb.webtransport:Connecting to localhost:4433

1. Simple calculations:
  5 + 3 = 8
  10 - 4 = 6
  7 * 6 = 42
  20 / 4 = 5.0

2. Concurrent requests (demonstrating multiplexing):
  100 + 50 = 150
  200 - 75 = 125
  25 * 4 = 100

✅ Demo completed successfully!
```

## WebTransport vs WebSocket

### WebTransport Advantages:

| Feature | WebTransport | WebSocket |
|---------|--------------|-----------|
| Protocol | HTTP/3/QUIC | HTTP/1.1/2 + upgrade |
| Multiplexing | Native (streams) | Manual implementation |
| Head-of-line blocking | No | Yes (on packet loss) |
| 0-RTT reconnection | Yes | No |
| Unidirectional streams | Yes | No |
| Datagrams | Yes | No |
| Setup handshake | 1-RTT (0-RTT on reconnect) | 2-RTT |

### When to Use WebTransport:

- **High-performance applications**: Gaming, real-time collaboration, live streaming
- **Lossy networks**: Mobile, satellite, poor WiFi (QUIC handles packet loss better)
- **Multiple concurrent streams**: Many parallel requests without head-of-line blocking
- **Modern browsers**: Chrome 97+, Edge 97+ (Firefox and Safari support coming)

### When to Use WebSocket:

- **Universal browser support**: Works everywhere
- **Simple bidirectional communication**: Single logical stream
- **Existing infrastructure**: Already using HTTP/2 or HTTP/1.1
- **Production stability**: More mature ecosystem

## Security Notes

**Development**:
- Self-signed certificates work fine
- Client must disable certificate verification or trust the CA

**Production**:
- Use properly CA-signed certificates (Let's Encrypt, etc.)
- Enable certificate verification (`verify_mode=True`)
- Never disable verification in production

## Protocol Details

WebTransport uses:
- **Transport**: QUIC over UDP
- **Application**: HTTP/3
- **Security**: TLS 1.3 (mandatory)
- **Port**: 4433 (example), 443 (standard HTTPS)

Data flow:
1. Client establishes QUIC connection with TLS 1.3 handshake
2. HTTP/3 connection initialized with `h3` ALPN
3. WebTransport session created over HTTP/3
4. Bidirectional streams used for RPC messages
5. Messages serialized as NDJSON (same as HTTP batch)

## Troubleshooting

### "WebTransport requires aioquic library"
```bash
uv pip install aioquic
```

### "Certificate verification failed"
- For development: Client uses `verify_mode=False` by default
- For production: Provide valid `cert_path` and enable `verify_mode=True`

### "Connection refused"
- Ensure server is running
- Check firewall allows UDP on port 4433
- Verify certificate files exist

### Port already in use
Change the port in both server.py and client.py:
```python
# server.py
port = 4434  # Use a different port

# client.py
url = "https://localhost:4434/rpc/wt"
```

## Further Reading

- [WebTransport Specification](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3/)
- [aioquic Documentation](https://aioquic.readthedocs.io/)
- [HTTP/3 Explained](https://http3-explained.haxx.se/)
- [QUIC Protocol](https://datatracker.ietf.org/doc/html/rfc9000)
