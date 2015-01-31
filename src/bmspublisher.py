
import tailer # https://github.com/six8/pytailer
import parse # https://github.com/r1chardj0n3s/parse
import argparse, sys, json
from datetime import datetime

try:
    import asyncio
except ImportError:
    import trollius as asyncio

from pyndn import Name, Data, ThreadsafeFace
from pyndn.security import KeyChain
from pyndn.security.identity import FilePrivateKeyStorage, BasicIdentityStorage
from pyndn.security.identity import IdentityManager
from pyndn.util.memory_content_cache import MemoryContentCache



def publish(logline, rootname, cache):
    global face, keychain
    # Pull out and parse datetime for log entry 
    # (note we shoudld use point time for timestamp)
    try:
        if not ": (point" in logline: return
        logdtstr = parse.search("[{}]", logline)[0]
        point = parse.search("(point {})", logline)[0].split(" ")
    except Exception as detail:
        print("publish: Parse error for", logline, "-", detail)
        return
    try:
        logdt = datetime.strptime(logdtstr, "%Y-%m-%d %H:%M:%S.%f")
    except Exception as detail:
        print("publish: Date/time conversion error for", logline, "-", detail)
        return
        
    name = pointNameToName(point[0], rootname)
    data_json, data_dict = pointToJSON(point)
    
    if name is not None:
        print("Publishing log entry", logdt, "to", name, data_dict["timestamp"], "payload:", data_json)
        try:
            cache.add(createData(name, data_dict["timestamp"], data_json))
        except Exception as detail:
            print("publish: Error calling createData for", logline, "-", detail)

def createData(name, timestamp, payload):
    data = Data( Name("ndn:"+name+"/"+str(timestamp)) ) 
    data.setContent(payload)
    keychain.sign(data, keychain.getDefaultCertificateName())
    #print(repr(data))
    return data

def pointNameToName(point, root):
    try: 
        comps = point.lower().split(":")[1].split(".")
        name = root+"/"+"/".join(comps)
    except Exception as detail:
        print("publish: Error constructing name for", point, "-", detail)
        return None
    return name

def pointToJSON(pd):
    d = {}
    args = ["pointname", "type", "value", "conf", "security", "locked", "seconds", "nanoseconds", "unknown_1", "unknown_2"]
    for i in range(len(args)):
        try:
            d[args[i]] = pd[i]
        except Exception as detail:
            d[args[i]] = None
            print("pointToJSON: Error parsing arg", args[i], "from", pd, "-", detail)
    try:
        timestamp = (int(d["seconds"])+int(d["nanoseconds"])*1e-9)
        dt = datetime.fromtimestamp(timestamp)
        d["timestamp_str"] = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        d["timestamp"] = str(timestamp)
    except Exception as detail:
        print("pointToJSON: Error in timestamp conversation of", pd)
        d["timestamp"] = 0
        d["timestamp_str"] = ("0000-00-00 00:00:00.00")
    try:
        djs = json.dumps(d)
    except Exception as detail:
        print("pointToJSON: Error in JSON conversation of", pd)
        return "{}"
    return djs, d

@asyncio.coroutine
def readfile(filename, namespace, cache):
    f = open(filename, 'r')
    for line in f:
        publish(line, namespace, cache)
    f.close()
    
@asyncio.coroutine
def followfile(filename, namespace):
    for line in tailer.follow(open(filename)):
        publish(line, namespace, cache)

        
def main(): 
  
    # COMMAND LINE ARGS
    parser = argparse.ArgumentParser(description='Parse or follow Cascade Datahub log and publish to NDN.')
    parser.add_argument('filename', help='datahub log file')
    parser.add_argument('-f', dest='follow', action='store_true',
                       help='follow (tail -f) the log file')  
    parser.add_argument('--namespace', default='/ndn/edu/ucla/remap/bms', 
                        help='root ndn name, no trailing slash')
    args = parser.parse_args()
  
    # NDN 
    global face, keychain
    loop = asyncio.get_event_loop()
    face = ThreadsafeFace(loop, "localhost")

    keychain = KeyChain(IdentityManager(BasicIdentityStorage(), FilePrivateKeyStorage() ))       # override default even for MacOS
    cache = MemoryContentCache(face)
  
    # READ THE FILE (MAIN LOOP)
    if args.follow: 
        loop.run_until_complete(followfile(args.filename, args.namespace, cache))
    else:
        loop.run_until_complete(readfile(args.filename, args.namespace, cache))

    face.shutdown()
        
if __name__ == '__main__':
    main()