import tarfile
import asyncio, telnetlib3
from os import environ
from time import sleep

PORT = environ["PORT"]

tar = tarfile.open("logs_tradesprod_2021-01-25.txt.tar", "r:gz")
for member in tar.getmembers():
    f = tar.extractfile(member)
    if f is not None:
        content = f.read().splitlines()
        
# for msg in content:
#     print(msg.decode('ascii') + "|", end='')

sleep(10)

# join into one huge message
for i in range(len(content)):
    content[i] = content[i].decode('ascii')

bigmsg = "|".join(content)


# @asyncio.coroutine
def shell(reader, writer):
    # writer.write('\r\nWould you like to play a game? ')
    # inp = yield from reader.read(1)
    writer.echo("connected, starting to spam that fucker")
    writer.write(bigmsg)
    writer.close()

loop = asyncio.get_event_loop()
coro = telnetlib3.create_server(host="0.0.0.0",port=PORT, shell=shell)
server = loop.run_until_complete(coro)
loop.run_until_complete(server.wait_closed())