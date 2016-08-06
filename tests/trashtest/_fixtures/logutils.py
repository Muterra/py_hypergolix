'''
Scratchpad for test-based development.

LICENSING
-------------------------------------------------

hypergolix: A python Golix client.
    Copyright (C) 2016 Muterra, Inc.
    
    Contributors
    ------------
    Nick Badger 
        badg@muterra.io | badg@nickbadger.com | nickbadger.com

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the 
    Free Software Foundation, Inc.,
    51 Franklin Street, 
    Fifth Floor, 
    Boston, MA  02110-1301 USA

------------------------------------------------------

'''


import sys
import pathlib
import logging
import datetime


def autoconfig(logdirname='logs', loglevel='debug'):
    fname = sys.argv[0]
    logdir = pathlib.Path(logdirname)

    if (not logdir.exists()) or (not logdir.is_dir()):
        logdir.mkdir()

    # Note that double slashes don't cause problems.
    prefix = logdirname + '/' + pathlib.Path(fname).stem
    ii = 0
    date = str(datetime.date.today())
    ext = '.pylog'
    while pathlib.Path(prefix + '_' + date + '_' + str(ii) + ext).exists():
        ii += 1
    logname = prefix + '_' + date + '_' + str(ii) + ext
    print('USING LOGFILE: ' + logname)

    # Calculate the logging level
    try:
        loglevel = {'debug': logging.DEBUG,
                    'info': logging.INFO,
                    'warning': logging.WARNING,
                    'error': logging.ERROR,}[loglevel.lower()]
    except KeyError:
        loglevel = logging.WARNING

    # Make a log handler
    loghandler = logging.FileHandler(logname)
    loghandler.setFormatter(
        logging.Formatter(
            '%(threadName)-7s %(name)-12s: %(levelname)-8s %(message)s'
        )
    )
    
    # Add to root logger
    logging.getLogger('').addHandler(loghandler)
    logging.getLogger('').setLevel(loglevel)
        
    # Silence the froth
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('websockets').setLevel(logging.WARNING)