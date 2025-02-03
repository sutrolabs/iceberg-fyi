import json
import logging
import os
import shutil
import subprocess
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup():
    if shutil.which("doppler"):
        is_configured = all(
            json.loads(
                subprocess.run(
                    ["doppler", "configure", "get", "project", "config", "--json"],
                    check=True,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                ).stdout
            ).values()
        )
        if is_configured:
            setup_doppler()
            return

    if os.path.exists(".env"):
        setup_dotenv()
        return

    logger.info("Using secrets from environment")


def setup_doppler():
    logger.info("Using secrets from Doppler")
    secrets = json.loads(
        subprocess.run(
            ["doppler", "secrets", "download", "--no-file", "--format=json"],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).stdout
    )
    for key, value in secrets.items():
        os.environ[key] = value


def setup_dotenv():
    logger.info("Using secrets from .env")
    load_dotenv()
