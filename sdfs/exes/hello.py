if __name__ == "__main__":
    filename ="demo.csv"

    file = open(filename, 'r')
    Lines = file.readlines()
    out = "output"
    idx_key = 10
    idx_val = 9
    param = "Fiber"
    key_v = {}
    for l in Lines:
        line = l.split(",")
        if line[idx_key] == param and line[idx_key] != "Interconne":
            if line[idx_val] not in key_v:
                key_v[line[idx_val]] = [1]
            else:
                key_v[line[idx_val]].append(1)

    for key in key_v:
        k = key.replace("/", ";")
        with open(out + "_" + k + ".txt", "a") as file:
            # fcntl.flock(file, fcntl.LOCK_EX)
            val = key_v[key]
            for v in val:
                to_write = key + ":" + str(v) + "\n"
                file.write(to_write)
            # fcntl.flock(file, fcntl.LOCK_UN)
