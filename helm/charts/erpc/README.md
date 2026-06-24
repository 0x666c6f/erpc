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

## Vault Config

The eRPC chart reads non-secret config from `vault.configPath` and secret values
from `vault.secretsPath`. Keep `vault.configPath` as a config template. Static
secret placeholders use `__SECRET_<KEY>__`; the API-key auth list can use the
`__SECRET_API_KEY_STRATEGIES__` marker to generate one secret auth strategy for
every `API_KEY_*=<value>` entry in `vault.secretsPath`. Generated auth IDs are
derived from the key name after `API_KEY_`, lowercased with `_` converted to `-`;
trailing hash rotation suffixes like `_9E41B694` are ignored so rotating a value
does not change the eRPC secret ID. Optional per-key metadata entries can tune
generated strategies without creating new API keys: `API_KEY_FOO__ID` pins the
eRPC secret ID, `API_KEY_FOO__ORDER` pins list order, `API_KEY_FOO__ALLOW_METHODS`
is a comma-separated method allowlist, and `API_KEY_FOO__RATE_LIMIT_BUDGET` sets
`rateLimitBudget`.

Vault secret keys must match `[A-Za-z_][A-Za-z0-9_]*`. Secret values are parsed
from a line-oriented `KEY=VALUE` file, so keep them single-line. Placeholders in
quoted YAML scalars are YAML-escaped by the renderer; placeholders embedded in
unquoted YAML scalars, such as URL path fragments, must have YAML-plain-safe
values.

The Vault config job renders placeholders into a temporary `erpc.yaml`, validates
that rendered config with `erpc validate`, then stores only the rendered file in
the runtime Kubernetes Secret.
