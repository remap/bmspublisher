
import tailer # https://github.com/six8/pytailer
import parse # https://github.com/r1chardj0n3s/parse
import argparse, sys, json
from datetime import datetime

def publish(logline, rootname):
    
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
        print(logdt, name, data_dict["timestamp"], data_json)
        
                            

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

def main(): 
  
    parser = argparse.ArgumentParser(description='Parse or follow Cascade Datahub log and publish to NDN.')
    parser.add_argument('filename', help='datahub log file')
    parser.add_argument('-f', dest='follow', action='store_true',
                       help='follow (tail -f) the log file')  
    parser.add_argument('--namespace', default='/ndn/edu/ucla/remap/bms', 
                        help='root ndn name, no trailing slash')
    args = parser.parse_args()
  
    if args.follow: 
        for line in tailer.follow(open(args.filename)):
            publish(line, args.namespace)
    else:
        f = open(args.filename, 'r')
        for line in f:
            publish(line, args.namespace)
        f.close()
        
if __name__ == '__main__':
    main()