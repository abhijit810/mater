"""Environment-driven configuration for the proxy app."""
import os

from dotenv import load_dotenv

# Load .env from the repo root if present (does not override real env vars).
load_dotenv()


class Config:
    BOOTSTRAP_SERVERS: str = os.getenv("CC_BOOTSTRAP_SERVERS", "")
    API_KEY: str = os.getenv("CC_API_KEY", "")
    API_SECRET: str = os.getenv("CC_API_SECRET", "")

    TELEMETRY_TOPIC: str = os.getenv("TELEMETRY_TOPIC", "vehicle_telemetry")

    HOST: str = os.getenv("PROXY_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PROXY_PORT", "8000"))

    @classmethod
    def validate(cls) -> None:
        missing = [
            name
            for name in ("BOOTSTRAP_SERVERS", "API_KEY", "API_SECRET")
            if not getattr(cls, name)
        ]
        if missing:
            raise RuntimeError(
                "Missing required Confluent Cloud config: "
                + ", ".join("CC_" + m for m in missing)
                + ". Copy .env.example to .env and fill in your values."
            )


config = Config()
