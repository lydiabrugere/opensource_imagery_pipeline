# Set default logging handler to avoid "No handler found" warnings.
import logging

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

__title__ = 'orthoimagery_pipeline'
__version__ = '0.4.dev0'
__all__ = ['define', 'download', 'evi_process', 'publish', 'validate']
