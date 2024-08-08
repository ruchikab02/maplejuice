import sys
# import fcntl

params = sys.argv[1:]

content = params[0]
filename = params[1]
key_v = {}
cont_split = content.split("\n")
for val in cont_split:
    cont_split2 = val.split(" ")
    for val2 in cont_split2:
        if not val2 == "":
            if not val2 in key_v:
                key_v[val2] = [1]
            else:
                key_v[val2].append(1)
# print(key_v)
for key in key_v:
    # TODO: add local/ back here, removed for testing
    with open("local/int_pref/" + filename + "_" + key + ".txt", "a") as file:
        # fcntl.flock(file, fcntl.LOCK_EX)
        val = key_v[key]
        for v in val:
            to_write = key + " : " + str(v) + "\n"
            file.write(to_write)
        # fcntl.flock(file, fcntl.LOCK_UN)