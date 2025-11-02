"""
Configuration loader with environment variable support.
Supports YAML files with ${VAR_NAME} substitution.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv


def substitute_env_vars(value: Any) -> Any:
    """
    Recursively substitute ${VAR_NAME} patterns with environment variables.

    Args:
        value: Configuration value (string, dict, list, etc.)

    Returns:
        Value with environment variables substituted.
    """
    if isinstance(value, str):
        # Replace ${VAR_NAME:default} or ${VAR_NAME} patterns
        def replace_var(match: Any) -> str:
            var_name = match.group(1)
            if ":" in var_name:
                name, default = var_name.split(":", 1)
                return os.getenv(name, default)
            return os.getenv(var_name, match.group(0))

        import re
        result = re.sub(r"\$\{([^}]+)\}", replace_var, value)

        # Convert string booleans to actual booleans
        if result.lower() == "true":
            return True
        elif result.lower() == "false":
            return False

        return result

    elif isinstance(value, dict):
        return {k: substitute_env_vars(v) for k, v in value.items()}

    elif isinstance(value, list):
        return [substitute_env_vars(v) for v in value]

    return value


class Config:
    """Configuration manager."""

    def __init__(self, config_dict: Dict[str, Any]) -> None:
        """
        Initialize config from dictionary.

        Args:
            config_dict: Configuration dictionary.
        """
        self._config = config_dict

    @classmethod
    def from_yaml(cls, path: Optional[str] = None) -> "Config":
        """
        Load configuration from YAML file with environment variable substitution.

        Args:
            path: Path to config.yaml. If None, uses ./config.yaml

        Returns:
            Config instance.

        Raises:
            FileNotFoundError: If config file not found.
        """
        # Load environment variables from .env file
        load_dotenv()

        if path is None:
            path = "./config.yaml"

        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path.absolute()}")

        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)

        # Substitute environment variables
        config_dict = substitute_env_vars(config_dict)

        return cls(config_dict)

    def __getattr__(self, name: str) -> Any:
        """Get configuration value or nested Config object."""
        if name.startswith("_"):
            return object.__getattribute__(self, name)

        if name in self._config:
            value = self._config[name]
            if isinstance(value, dict):
                return Config(value)
            return value

        raise AttributeError(f"Config has no attribute: {name}")

    def __getitem__(self, key: str) -> Any:
        """Get configuration value using dictionary access."""
        if key in self._config:
            value = self._config[key]
            if isinstance(value, dict):
                return Config(value)
            return value

        raise KeyError(f"Config has no key: {key}")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value with default.

        Args:
            key: Configuration key.
            default: Default value if key not found.

        Returns:
            Configuration value or default.
        """
        if key in self._config:
            value = self._config[key]
            if isinstance(value, dict):
                return Config(value)
            return value
        return default

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return self._config

    def __repr__(self) -> str:
        return f"Config({self._config})"


def load_config(path: Optional[str] = None) -> Config:
    """
    Load configuration from YAML file.

    Args:
        path: Path to config.yaml. If None, uses ./config.yaml

    Returns:
        Config instance.
    """
    return Config.from_yaml(path)
