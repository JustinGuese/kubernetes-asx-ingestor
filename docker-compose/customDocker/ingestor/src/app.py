import telnetlib
from os import environ

tn_ip = environ["TELNETSERVER"]
tn_port = str(environ["TELNETPORT"])

def telnet():
    try:
        tn = telnetlib.Telnet(tn_ip, tn_port, 15)
    except:
        raise
    # tn.set_debuglevel(100)
    while True:
        msg = tn.read_until(b"|")
        msg = msg.decode("ascii")[:-1] # split the | off
        print(msg)

telnet()