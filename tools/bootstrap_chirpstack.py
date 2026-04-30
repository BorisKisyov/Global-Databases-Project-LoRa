import json
import re
import subprocess
import sys
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise SystemExit(result.returncode)
    return result.stdout.strip()


def update_env(updates: dict[str, str]) -> None:
    text = ENV_PATH.read_text(encoding="utf-8")
    for key, value in updates.items():
        pattern = re.compile(rf"^{re.escape(key)}=.*$", re.MULTILINE)
        replacement = f"{key}={value}"
        if pattern.search(text):
            text = pattern.sub(replacement, text)
        else:
            text += f"\n{replacement}\n"
    ENV_PATH.write_text(text, encoding="utf-8")


def main() -> None:
    print("Creating ChirpStack API key...")
    out = run([
        "docker", "compose", "exec", "-T", "chirpstack",
        "chirpstack", "--config", "/etc/chirpstack", "create-api-key", "--name", "GD-simulator"
    ])
    token_match = re.search(r"token:\s*(.+)", out)
    if not token_match:
        print(out)
        raise SystemExit("Could not parse API token from chirpstack output.")
    token = token_match.group(1).strip()

    print("Querying ChirpStack tenants...")
    req = Request(
        "http://localhost:8090/api/tenants?limit=100&offset=0",
        headers={
            "accept": "application/json",
            "Grpc-Metadata-Authorization": f"Bearer {token}",
        },
    )
    with urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    result = data.get("result") or []
    if not result:
        raise SystemExit("No tenants returned from ChirpStack REST API.")
    tenant_id = result[0]["id"]

    update_env({
        "CHIRPSTACK_API_KEY": token,
        "CHIRPSTACK_TENANT_ID": tenant_id,
    })

    print("Updated .env successfully.")
    print(f"CHIRPSTACK_TENANT_ID={tenant_id}")
    print("Now run: docker compose --profile simulator up -d --build chirpstack-simulator")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
