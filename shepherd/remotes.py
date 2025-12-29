"""Manage per-remote configuration (conda env, etc.)."""

from shepherd import constants
from shepherd import fs


# ============================================================
# Per-remote config (stored locally, for remote hosts)
# ============================================================

def load_remotes():
    """Load remotes config."""
    data = fs.read_json(constants.REMOTES_PATH)
    if data is None or (isinstance(data, dict) and data.get("_corrupt")):
        return {}
    return data


def save_remotes(data):
    """Save remotes config."""
    fs.atomic_write_json(constants.REMOTES_PATH, data)


def get_remote_config(remote_name):
    """Get config for a specific remote."""
    data = load_remotes()
    return data.get(remote_name, {})


def set_remote_config(remote_name, key, value):
    """Set a config value for a remote."""
    data = load_remotes()
    if remote_name not in data:
        data[remote_name] = {}
    data[remote_name][key] = value
    save_remotes(data)
    return data[remote_name]


def get_conda_env(remote_name):
    """Get conda env for a remote."""
    config = get_remote_config(remote_name)
    return config.get("conda_env")


def set_conda_env(remote_name, conda_env):
    """Set conda env for a remote."""
    return set_remote_config(remote_name, "conda_env", conda_env)


def get_conda_activation_script(remote_name):
    """Generate shell commands to activate conda env for a remote."""
    conda_env = get_conda_env(remote_name)
    if not conda_env:
        return ""
    return _make_conda_activation_script(conda_env)


# ============================================================
# Local config (stored on remote host, used by daemon)
# ============================================================

def load_local_config():
    """Load local config (used on remote host)."""
    data = fs.read_json(constants.LOCAL_CONFIG_PATH)
    if data is None or (isinstance(data, dict) and data.get("_corrupt")):
        return {}
    return data


def save_local_config(data):
    """Save local config."""
    fs.atomic_write_json(constants.LOCAL_CONFIG_PATH, data)


def get_local_config(key, default=None):
    """Get a local config value."""
    config = load_local_config()
    return config.get(key, default)


def set_local_config(key, value):
    """Set a local config value."""
    config = load_local_config()
    config[key] = value
    save_local_config(config)
    return config


def get_local_conda_env():
    """Get the local conda env (used by daemon on remote host)."""
    return get_local_config("conda_env")


def get_local_conda_activation_script():
    """Get conda activation script for local use (by daemon)."""
    conda_env = get_local_conda_env()
    if not conda_env:
        return ""
    return _make_conda_activation_script(conda_env)


def _make_conda_activation_script(conda_env):
    """Generate shell commands to activate a conda env."""
    # Support micromamba, mamba, and conda (in order of preference)
    return f"""
# Activate conda environment
if command -v micromamba &> /dev/null; then
    eval "$(micromamba shell hook --shell bash)"
    micromamba activate {conda_env}
elif command -v mamba &> /dev/null; then
    eval "$(mamba shell hook --shell bash)"
    mamba activate {conda_env}
elif command -v conda &> /dev/null; then
    eval "$(conda shell.bash hook)"
    conda activate {conda_env}
else
    echo "Warning: micromamba/mamba/conda not found, skipping environment activation" >&2
fi
"""
