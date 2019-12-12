import toml
import os
import re
import os.path

here = os.path.abspath(os.path.join(__file__, os.pardir, os.pardir))

f = os.path.join(here, "tesseract-server", "Cargo.toml")
res = toml.load(open(f))

# capture current version
current_v = res['package']['version']

# increment
base_line, v = current_v.rsplit(".", 1)
new_v = "{}.{}".format(base_line, int(v) + 1)

res['package']['version'] = new_v
toml.dump(res, open(f, 'w'))

# run cargo build
f = os.path.join(here)
print("cd {0}; cargo build; git add .; git commit -m'v{1}'; git tag -a v{1} -m 'v{1}'; git push --tags; git push".format(f, new_v))


