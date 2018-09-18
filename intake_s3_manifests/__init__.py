# -*- coding: utf-8 -*-
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .s3_manifest import S3ManifestSource

import intake.container
