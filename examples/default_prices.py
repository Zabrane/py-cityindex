
import sys

import cityindex as ci

api = ci.CiApiClient('', '')
strm = ci.CiStreamingClient(api)


import logging
logging.basicConfig(level=logging.DEBUG)
strm.default.listen(lambda *args: sys.stdout.write('%s\n' % (args,)),
                    ci.OPERATOR_IFX_POLAND)
raw_input()
strm.stop()
