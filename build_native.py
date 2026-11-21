"""Build the Linux native engine against Arrow/Parquet shipped by PyArrow."""
from pathlib import Path
import os
import shutil
import subprocess
import pyarrow

root = Path(__file__).resolve().parent
libdir = Path(pyarrow.__file__).resolve().parent
(root / 'lib').mkdir(exist_ok=True)
libs = []
for name in ('libarrow.so.*', 'libparquet.so.*'):
    source = next(p for p in libdir.glob(name) if p.is_file())
    dest = root / 'lib' / source.name
    shutil.copy2(source, dest)
    libs.append(str(dest))
flags = os.environ.get('NATIVE_FLAGS', '-O3 -DNDEBUG -march=x86-64-v3').split()
subprocess.run(['g++', '-std=c++20', *flags, '-pthread', '-I' + pyarrow.get_include(),
                str(root / 'src' / 'engine.cpp'), *libs, '-Wl,-rpath,$ORIGIN/lib',
                '-o', str(root / 'engine')], check=True)
print('Built:', root / 'engine')
