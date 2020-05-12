filepath = 'killedpulse100-300_c=5_c=3.txt'
count=0;
with open(filepath) as fp:
   line = fp.readline()
   cnt = 1
   dead=0
   while line:
       #print("Line {}: {}".format(cnt, line.strip()))
       x = line.find("t:")
       time = int(line[(x+4):])
       print(time)
       count = count + time;
       print("Timepoints' sum: ", count)
       line = fp.readline()
       cnt += 1
       dead += 1 

print("Finally died: ", dead)
