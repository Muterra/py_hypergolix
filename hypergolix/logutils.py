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
import logging.handlers
import datetime
import time


def _make_logpath(root, prefix, name, suffix, ext):
    ''' Creates a logname using the passed prefix, suffix, ext, etc.
    Returns a path.
    '''
    if prefix != '':
        prefix = prefix + '_'
        
    if suffix != '':
        suffix = '_' + suffix
        
    if not ext.startswith('.'):
        ext = '.' + ext
        
    start_time = datetime.datetime.now()
    start_time = start_time.strftime('%Y.%m.%d_%H.%M.%S')
    
    return root / (prefix + name + suffix + '_' + start_time + ext)


def autoconfig(tofile=True, logdirname='logs', loglevel='warning', prefix='',
               logname=None, suffix=''):
    if tofile:
        # Use the name of the called script as the log title if none was given
        if logname is None:
            fname = sys.argv[0]
            logname = pathlib.Path(fname).stem
        
        # Convert the log directory to an absolute path
        logdir = pathlib.Path(logdirname).absolute()

        if (not logdir.exists()) or (not logdir.is_dir()):
            logdir.mkdir(parents=True)
            
        logpath = _make_logpath(
            root = logdir,
            prefix = '',
            name = logname,
            suffix = suffix,
            ext = '.pylog'
        )
        
        while logpath.exists():
            # Wait until the time changes, because, yeah.
            time.sleep(1)
            logpath = _make_logpath(
                root = logdir,
                prefix = '',
                name = logname,
                suffix = suffix,
                ext = '.pylog'
            )

        # Make a log handler
        loghandler = logging.handlers.RotatingFileHandler(
            filename = str(logpath),
            # This determines the max size of each log file, and the number of
            # log files to keep. To be clear, this is a shitload of logging.
            maxBytes = 2**20,
            backupCount = 10,
            # Setting this to True will defer log creation until it's needed
            delay = False
        )
        
    else:
        loghandler = logging.StreamHandler()
        logname = None
        
    loghandler.setFormatter(
        logging.Formatter(
            '%(threadName)-10.10s '
            '%(name)-17.17s  %(levelname)-5.5s  '
            '%(asctime)s %(message)s'
        )
    )
    
    # Add to root logger
    logging.getLogger('').addHandler(loghandler)

    # Calculate the logging level
    loglevel = loglevel.lower()
    loglevel_enum = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'shouty': logging.ERROR,
        'extreme': logging.ERROR
    }
    try:
        log_setpoint = loglevel_enum[loglevel]
    except KeyError:
        log_setpoint = logging.WARNING
        
    # Silence the froth but keep the good stuff
    logging.getLogger('').setLevel(log_setpoint)
    
    if loglevel == 'shouty':
        logging.getLogger('asyncio').setLevel(logging.INFO)
        logging.getLogger('websockets').setLevel(logging.DEBUG)
        
    elif loglevel == 'extreme':
        logging.getLogger('asyncio').setLevel(logging.DEBUG)
        logging.getLogger('websockets').setLevel(logging.DEBUG)
        
    else:
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)
        
    return logname
