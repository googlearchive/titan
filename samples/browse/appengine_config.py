#!/usr/bin/env python
"""appengine_config.py to register the directory mixin."""

from titan import files
from titan.files import dirs

files.RegisterFileMixins([dirs.DirManagerMixin])
