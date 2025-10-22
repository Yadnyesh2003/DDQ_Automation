
import os
import yaml
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path


def load_yaml_config(file_path: str) -> dict:
    try:
        with open(file_path, "r") as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"⚠️ Config file not found at {file_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"❌ Error parsing YAML file {file_path}: {str(e)}")


def load_env_file(env_path: str = ".env") -> dict:
    if not Path(env_path).exists():
        raise FileNotFoundError(f"⚠️ Missing .env file at {env_path}")
    load_dotenv(dotenv_path=env_path)
    env_vars = {
        "CLIENT_NAME": os.getenv("CLIENT_NAME"),
        "CONFIG_ENV": os.getenv("CONFIG_ENV"),
        "DATA_PATH": os.getenv("DATA_PATH", "data/input/"),
        "INTERMEDIATE_PATH": os.getenv("INTERMEDIATE_PATH", "data/intermediate/"),
        "OUTPUT_PATH": os.getenv("OUTPUT_PATH", "data/output/")
    }
    today_str = os.getenv("TODAY", "")
    if today_str:
        try:
            # Expecting dd-mm-yyyy format like 17-10-2025
            env_vars["TODAY"] = datetime.strptime(today_str, "%d-%m-%Y").date()
        except ValueError:
            raise ValueError(f"❌ Invalid TODAY format: '{today_str}'. Expected format: 'dd-mm-yyyy'")
    else:
        env_vars["TODAY"] = datetime.today().date()
    # sanity check
    missing = [k for k, v in env_vars.items() if v is None]
    if missing:
        raise EnvironmentError(f"❌ Missing required environment variables: {missing}")
    return env_vars


def load_config() -> tuple:
    root_dir = Path(__file__).resolve().parents[1]
    env_path = root_dir / "config" / ".env"
    main_config_path = root_dir / "config" / "config.yaml"
    #client_config_path = root_dir / "config" / "client_config.yaml"
    env = load_env_file(env_path)
    main_config = load_yaml_config(main_config_path)
    env_config_key = env["CONFIG_ENV"]
    if env_config_key not in main_config:
        raise KeyError(f"❌ Config environment '{env_config_key}' not found in config.yaml")
    config = main_config[env_config_key]

    # Load client configuration
    #client_config = load_yaml_config(client_config_path)
    #client_name = env["CLIENT_NAME"]
    # if client_name not in client_config:
    #     raise KeyError(f"❌ Client '{client_name}' not found in client_config.yaml")
    # client_conf = client_config[client_name
    # return config, client_conf, env
    
    return config, env
