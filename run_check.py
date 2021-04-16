import psutil
import socket
import winwifi
import subprocess
import runpy
import os
import datetime as dt
import time
import sys

with open(r'filepath\for\pid_log.txt', 'r') as pid_log:
    current_pid = int(pid_log.readlines()[-1])
    
with open(r'filepath\for\run_check_log.txt', 'a') as run_check_log:
    run_check_log.write(f'\nrun_check ran at {dt.datetime.now()}\n')
    
print('The current PID is', current_pid)

def check_tradebot_running(tradebot_pid):
    #Iterate over all the running processes to see if any match tradebot's most recent pid
    for proc in psutil.process_iter():
        try:
            if proc.pid == tradebot_pid:
                print('Trade Bot if running!')
                return True            
            
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False

def check_internet_connection():
    IPaddress = socket.gethostbyname(socket.gethostname())
    if IPaddress == "127.0.0.1":
        print('NO INTERNET, running on localhost', IPaddress)
        return False
    else:
        print('Internet connected with IP address', IPaddress)
        return True

# Check if the tradebot process is running or not,
# if successful, will enter result into the run check log txt file.
run_check = check_tradebot_running(current_pid)
if run_check:
    print('Run check passed')
    with open(r'filepath\for\run_check_log.txt', 'a') as run_check_log:
        run_check_log.write('run_check passed\n')
    sys.exit('exiting...')

# if tradebot is not running, will check if internet is connected,
# if not, it will connect to preferred SSID, try to run tradebot,
# and check if tradebot is successfully running now.
# if the run check fails again, it will wait 60 seconds, try to connect to the internet again,
# attempt to run tradebot a final time, and check to see if this time was successful
# finally, if the run check fails a 3rd time, an error is written into the
# error log txt file along with the time it failed to run.
else:
    print('Trade Bot is not currently running')
    print('Checking internet connection')
    
    connection_check = check_internet_connection()
    
    if connection_check:
        print('Trying to run Trade Bot')
        exec(open('tradebot.py').read())
    
    else:
        print('connecting to wifi...')
        winwifi.WinWiFi.connect('name_of_preferred_SSID')
        newIP = socket.gethostbyname(socket.gethostname())
        print('Now connected to ' + newIP)
        exec(open('tradebot.py').read())
    
    print('Checking Trade Bot again...')
    run_check2 = check_tradebot_running(current_pid)
    if run_check2:
        print('Program now running')
        with open(r'filepath\for\run_check_log.txt', 'a') as run_check_log:
            run_check_log.write('run_check passed\n')
        sys.exit('now exiting...')
        
    else:
        connection_check2 = check_internet_connection()
        if not connection_check2:
            print('sleeping for 60 seconds and trying again...')
            time.sleep(60)
            print('trying again to connect to wifi...')
            winwifi.WinWiFi.connect('name_of_preferred_SSID')
            newIP = socket.gethostbyname(socket.gethostname())
            print('Now connected to ' + newIP)
            exec(open('tradebot.py').read())
            
        run_check3 = check_tradebot_running(current_pid)
        if not run_check3:
            print('Failed to run program, logging the error')
            with open(r'filepath\for\error_log.txt', 'a') as error_log:
                error_log.write(f'Error: could not connect at, {dt.datetime.now()}\n\n')
