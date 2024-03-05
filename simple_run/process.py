import os
from subprocess import Popen
from time import sleep
devnull = open(os.devnull, 'wb') # Use this in Python < 3.3
# Python >= 3.3 has subprocess.DEVNULL
Popen(['python3', 'subprocess_.py'], stdout=devnull, stderr=devnull)

print('subprocess_.py is running in the background')
sleep(20)