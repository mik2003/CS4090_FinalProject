def is_valid_username(username: str) -> bool:
    return (
        3 <= len(username) <= 32
        and username[0].isalpha()
        and all(c.isalnum() or c in "_-" for c in username)
    )
