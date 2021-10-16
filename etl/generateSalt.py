import random

ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def generate_salt():
    """
    Description: Salt function that generates a random string of 16 characters
    
    Arguments:
        None
    
    Returns:
        Salt string e.g "MO0kdqaPGLnWgN2n"
    """
    return ''.join(random.choice(ALPHABET) for i in range(16))