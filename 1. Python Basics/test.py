from os.path import join, dirname, realpath

path = dirname(realpath(__file__))

path_join = join(path, "mypackage")

print(path_join)

import sys

sys.path.append(path_join)

from mypackage import mymodule
