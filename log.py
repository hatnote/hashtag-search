import os

from lithoxyl import Logger, SensibleSink, SensibleFormatter, StreamEmitter, SensibleFilter
from lithoxyl.emitters import FileEmitter

class FixedFileEmitter(FileEmitter):
    def __init__(self, filepath, encoding=None, **kwargs):
        self.encoding = encoding
        super(FixedFileEmitter, self).__init__(filepath, encoding, **kwargs)

LOGFILE = 'server.log'  # TODO: where?

tlog = Logger('toplog')

file_fmt = SensibleFormatter('{status_char}{end_local_iso8601_noms_notz} - {duration_s\
ecs}s - {record_name} - {message}')
file_emt = FixedFileEmitter(LOGFILE)
file_filter = SensibleFilter(success='debug',
                              failure='debug',
                              exception='debug')
file_sink = SensibleSink(formatter=file_fmt,
                         emitter=file_emt,
                         filters=[file_filter])
tlog.add_sink(file_sink)
