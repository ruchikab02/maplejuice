import sys
params = sys.argv[1:]

content = params[0]
filename = params[1]
filt = params[2]
idx_key = 10
idx_val = 9
conts = content.split("\n")
key_v = {}

for s in conts:
    line = s.split(",")
    # print("line: ", line)
    # print("line idx: ", line[idx_key])
    if idx_key < len(line):
        if line[idx_key] == filt and line[idx_key] != "Interconne":
            if not line[idx_val] in key_v:
                key_v[line[idx_val]] = [1]
            else:
                key_v[line[idx_val]].append(1)

for key in key_v:
    k = key.replace("/", ";")
    if key == "":
        k = "empty"
    with open("local/int_pref/" + filename + "_" + k + ".txt", "a") as file:
        # fcntl.flock(file, fcntl.LOCK_EX)
        val = key_v[key]
        for v in val:
            to_write = key + ":" + str(v) + "\n"
            file.write(to_write)
        # fcntl.flock(file, fcntl.LOCK_UN)