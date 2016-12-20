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
import argparse
import unittest
import sys

# ###############################################
# Testing
# ###############################################
                
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Hypergolix trashtest.')
    parser.add_argument(
        '--traceur',
        action = 'store_true',
        help = 'Enable thorough analysis, including stack tracing. '
    )
    parser.add_argument(
        '--debug',
        action = 'store_true',
        help = 'Currently unused.'
    )
    parser.add_argument(
        '--logdir',
        action = 'store',
        default = None,
        type = str,
        help = 'Log to a specified directory, relative to current path.'
    )
    parser.add_argument(
        '--verbosity',
        action = 'store',
        default = 'warning',
        type = str,
        help = 'Specify the logging level. '
               '"debug" -> most verbose, '
               '"info" -> somewhat verbose, '
               '"warning" -> default python verbosity, '
               '"error" -> quiet.',
    )
    parser.add_argument('unittest_args', nargs='*')

    args, unittest_args = parser.parse_known_args()
    
    # Do logging config and stuff
    from hypergolix import logutils
    
    if args.logdir:
        logutils.autoconfig(
            tofile = True,
            logdirname = args.logdir,
            loglevel = args.verbosity
        )
    else:
        logutils.autoconfig(
            tofile = False,
            loglevel = args.verbosity
        )
    
    # Dammit unittest using argparse
    # sys.argv[1:] = args.unittest_args
    from trashtest import *
    
    if args.traceur:
        # This diagnoses deadlocks
        from hypergolix.utils import TraceLogger
        with TraceLogger(interval=5):
            unittest.main(argv=sys.argv[:1] + unittest_args)
    
    else:
        unittest.main(argv=sys.argv[:1] + unittest_args)
