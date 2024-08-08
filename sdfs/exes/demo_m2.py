import sys
params = sys.argv[1:]
content = params[0]
filename = params[1]
filt = params[2]

conts = content.split("\n")
total = 0
for s in conts:
    line = s.split(":")
    if len(line) > 1:
        total += int(line[1].strip())
        print(line)

key_v = {}
for v in conts:
    line = v.split(":")
    if len(line) > 1:
        if line[0] not in key_v:
            key_v[line[0]] = int(line[1].strip()) / total

for key in key_v:
    k = key.replace("/", ";")
    with open("local/int_pref/" + filename + "_" + k + ".txt", "a") as file:
        val = key_v[key]
        to_write = key + ":" + str(val) + "\n"
        file.write(to_write)