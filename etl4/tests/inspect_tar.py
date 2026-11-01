import sys
import tarfile
p = sys.argv[1]
with tarfile.open(p, "r") as tf:
    members = tf.getmembers()
    print(f"{len(members)} members in {p}:")
    for m in members[:10]:
        print(f"  size={m.size:>12,}  name={m.name!r}")
