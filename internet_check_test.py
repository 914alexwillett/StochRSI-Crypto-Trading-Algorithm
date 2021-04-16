import socket
import winwifi

IPaddress = socket.gethostbyname(socket.gethostname())
# 127.0.0.1 indicates being connected to localhost, 
# thus not connected to the internet
if IPaddress == "127.0.0.1":
    print('No internet, you localhost is '+ IPaddress)
    connected = False
else:
    print('Internet connected with IP address ' + IPaddress)
    connected = True

if not connected:
    print('connecting to wifi...')
    winwifi.WinWiFi.connect('name_of_preferred_SSID')
    newIP = socket.gethostbyname(socket.gethostname())
    print('Now connected to ' + newIP)