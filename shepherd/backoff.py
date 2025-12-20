def compute_backoff(restart_count, base_sec=10, max_sec=300):
    if restart_count <= 0:
        return 0
    exponent = min(restart_count, 6)
    value = base_sec * (2 ** exponent)
    if max_sec is not None:
        value = min(value, max_sec)
    return int(value)
