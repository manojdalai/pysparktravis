
path = "logs/application.log"
print(path)

with open(path, 'r') as f:
    for line in f:
        if not line.strip():
            continue
        if line:
            print(line)