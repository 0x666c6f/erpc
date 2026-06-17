# eRPC Helm Chart

This Helm chart deploys eRPC as a fault-tolerant EVM RPC proxy service within the Morpho infrastructure.

## Local Development with Docker Compose

For local testing and development:

```bash
# Navigate to chart directory
cd charts/erpc/

# Copy environment template and edit with your API keys
cp .env.example .env
# Edit .env with your actual API keys and AUTH_SECRET

# Start services (Redis + eRPC)
docker-compose up -d

# Run tests
./test-local.sh

# Stop services
docker-compose down
```


## Usage

When authentication is enabled, requests must include the secret as a query parameter:

```bash
curl --location 'http://localhost:4000/cache/evm/1?secret=YOUR-AUTH-SECRET' \
     --header 'Content-Type: application/json' \
     --data '{"method":"eth_blockNumber","params":[],"id":1,"jsonrpc":"2.0"}'
```
