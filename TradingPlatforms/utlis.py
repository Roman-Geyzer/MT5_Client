# utils.py
"""
This module provides utility functions and classes for trading strategies, including configuration loading, 
timeframe and symbol magic number retrieval, and safe type conversions.
Classes:

Functions:

"""

import pandas as pd
import time
from datetime import datetime


retry_sets = [
    {"attempts": 3, "wait": 0},    # First set: no wait
    {"attempts": 3, "wait": 0.1},  # Second set: wait 0.1 seconds
    {"attempts": 3, "wait": 0.6},  # Third set: wait 0.6 seconds
    {"attempts": 3, "wait": 1.1},  # Fourth set: wait 1.1 seconds
]




def attempt_i_times_with_s_seconds_delay(i, s, loop_error_msg, func_check_func, func, args_tuple):
    """
    Attempt to execute a function a specified number of times with a delay between attempts.
    This function is useful for handling intermittent connection issues or other transient errors.
    
    Args:
        i (int): Number of attempts to make.
        s (float): Number (or part of 1) of seconds to wait between attempts.
        func_check_func (function): Function to check the result of the function.
        func (function): Function to execute.
        loop_error_msg (str): Error message to display on each failed attempt.
        final_error_msg (str): Error message to display if all attempts fail.
        args_tuple (tuple): Tuple of arguments to pass to the function.
    
    Returns:
        Any: Result of the function execution.
    """
    for attempt in range(i):
        result = func(*args_tuple)
        if func_check_func(result):  
            return result
        print(f"{loop_error_msg} on attempt {attempt + 1}")
        time.sleep(s)
    return result

def catch_i_times_with_s_seconds_delay(i, s , loop_error_msg, final_error_msg, func, *args):
    """
    Attempt to execute a function a specified number of times with a delay between attempts.
    Args:
        i (int): Number of attempts to make.
        s (float): Number (or part of 1) of seconds to wait between attempts.
        func (function): Function to execute.
        loop_error_msg (str): Error message to display on each failed attempt.
        final_error_msg (str): Error message to display if all attempts fail.
        *args: Arguments to pass to the function.
    Returns:
        Any: Result of the function execution.
    """
    print("attempt_i_times_with_s_seconds_delay")
    print(f"i = {i}, s = {s}, loop_error_msg = {loop_error_msg}, final_error_msg = {final_error_msg}")
    print(f"func = {func}, args = {args}")
    for attempt in range(i):
        try:
            return func(*args)
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {e}")
            time.sleep(s)
    return func(*args)

space = 150
hashes = 5
def print_current_time():
    current_time_str = f"current_time is: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    spaces = (space - len(current_time_str)) // 2
    print("#"*hashes + " " * spaces + current_time_str + " " * spaces + "#"*hashes)

def print_hashtags():
    print("#" * (space + 2*hashes))

def print_hashtaged_msg(hashed_lines, *args):
    print("\n")
    for _ in range(hashed_lines):
        print_hashtags()
    print_current_time()
    for arg in args:
        # Ensure arg is iterable
        if isinstance(arg, dict):
            items = arg.items()
        elif isinstance(arg, (tuple, list)):
            items = arg
        else:
            items = (arg,)
        
        for item in items:
            if isinstance(item, tuple) and len(item) == 2:
                key, value = item
                item_str = f"{key}: {value}"
            else:
                item_str = str(item)
            
            spaces = (space - len(item_str)) // 2
            print("#"*hashes + " " * spaces + item_str + " " * spaces + "#"*hashes)
    for _ in range(hashed_lines):
        print_hashtags()
