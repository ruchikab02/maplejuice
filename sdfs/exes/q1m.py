import re
import sys
params = sys.argv[1:]

content = params[0]
filename = params[1]
filt = params[2]
reg_exp = re.compile(filt)
key_v = {}
cont_split = content.split("\n")

for val in cont_split:
    if bool(re.match(reg_exp, val)):
        if not filt in key_v:
            key_v[filt] = [val]
        else:
            key_v[filt].append(val)
                
for key in key_v:
    k = key.replace("/", ";")
    with open("local/int_pref/" + filename + "_" + k + ".txt", "a") as file:
        val = key_v[key]
        for v in val:
            to_write = str(v) + "\n"
            file.write(to_write)