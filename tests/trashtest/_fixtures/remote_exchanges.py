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


from .identities import TEST_AGENT1
from .identities import TEST_AGENT2
from .identities import TEST_AGENT3

from .identities import TEST_READER1
from .identities import TEST_READER2
from .identities import TEST_READER3

        
# ---------------------------------------
# Create and publish identity containers
gidc1 = TEST_READER1.packed
gidc2 = TEST_READER2.packed


# ---------------------------------------
# Prerequisites for testing -- should move to setUp?
# Make some objects for known IDs
pt1 = b'[[ Hello, world? ]]'
pt2 = b'[[ Hiyaback! ]]'
secret1_1 = TEST_AGENT1.new_secret()
cont1_1 = TEST_AGENT1.make_container(
    secret = secret1_1,
    plaintext = pt1
)
secret1_2 = TEST_AGENT1.new_secret()
cont1_2 = TEST_AGENT1.make_container(
    secret = secret1_2,
    plaintext = pt2
)

secret2_1 = TEST_AGENT2.new_secret()
cont2_1 = TEST_AGENT2.make_container(
    secret = secret2_1,
    plaintext = pt1
)
secret2_2 = TEST_AGENT2.new_secret()
cont2_2 = TEST_AGENT2.make_container(
    secret = secret2_2,
    plaintext = pt2
)

# Make some objects for an unknown ID
secret3_1 = TEST_AGENT3.new_secret()
cont3_1 = TEST_AGENT3.make_container(
    secret = secret3_1,
    plaintext = pt1
)
secret3_2 = TEST_AGENT3.new_secret()
cont3_2 = TEST_AGENT3.make_container(
    secret = secret3_2,
    plaintext = pt2
)

# Make some bindings for known IDs
bind1_1 = TEST_AGENT1.make_bind_static(
    target = cont1_1.ghid
)
bind1_2 = TEST_AGENT1.make_bind_static(
    target = cont1_2.ghid
)

bind2_1 = TEST_AGENT2.make_bind_static(
    target = cont2_1.ghid
)
bind2_2 = TEST_AGENT2.make_bind_static(
    target = cont2_2.ghid
)

# Make some bindings for the unknown ID
bind3_1 = TEST_AGENT3.make_bind_static(
    target = cont3_1.ghid
)
bind3_2 = TEST_AGENT3.make_bind_static(
    target = cont3_2.ghid
)

# Make requests between known IDs
handshake1_1 = TEST_AGENT1.make_request(
    recipient = TEST_READER2,
    request = TEST_AGENT1.make_handshake(
        target = cont1_1.ghid,
        secret = secret1_1
    )
)

handshake2_1 = TEST_AGENT2.make_request(
    recipient = TEST_READER1,
    request = TEST_AGENT2.make_handshake(
        target = cont2_1.ghid,
        secret = secret2_1
    )
)

# Make a request to an unknown ID
handshake3_1 = TEST_AGENT1.make_request(
    recipient = TEST_READER3,
    request = TEST_AGENT1.make_handshake(
        target = cont1_1.ghid,
        secret = secret1_1
    )
)

# Make some debindings for those requests
degloveshake1_1 = TEST_AGENT2.make_debind(
    target = handshake1_1.ghid
)
degloveshake2_1 = TEST_AGENT1.make_debind(
    target = handshake2_1.ghid
)

# Make some dynamic bindings!
dyn1_1a = TEST_AGENT1.make_bind_dynamic(
    target = cont1_1.ghid
)
dyn1_1b = TEST_AGENT1.make_bind_dynamic(
    target = cont1_2.ghid,
    ghid_dynamic = dyn1_1a.ghid_dynamic,
    history = [dyn1_1a.ghid]
)

dyn2_1a = TEST_AGENT2.make_bind_dynamic(
    target = cont2_1.ghid
)
dyn2_1b = TEST_AGENT2.make_bind_dynamic(
    target = cont2_2.ghid,
    ghid_dynamic = dyn2_1a.ghid_dynamic,
    history = [dyn2_1a.ghid]
)

dyn3_1a = TEST_AGENT3.make_bind_dynamic(
    target = cont3_1.ghid
)
dyn3_1b = TEST_AGENT3.make_bind_dynamic(
    target = cont3_2.ghid,
    ghid_dynamic = dyn3_1a.ghid_dynamic,
    history = [dyn3_1a.ghid]
)

# And make some fraudulent ones
dynF_1b = TEST_AGENT1.make_bind_dynamic(
    target = cont1_2.ghid,
    ghid_dynamic = dyn2_1a.ghid_dynamic,
    history = [dyn2_1a.ghid]
)
dynF_2b = TEST_AGENT2.make_bind_dynamic(
    target = cont2_2.ghid,
    ghid_dynamic = dyn1_1a.ghid_dynamic,
    history = [dyn1_1a.ghid]
)

dynF_a = TEST_AGENT1.make_bind_dynamic(
    target = cont1_2.ghid
)
dynF_b = TEST_AGENT2.make_bind_dynamic(
    target = cont2_2.ghid,
    ghid_dynamic = dynF_a.ghid_dynamic,
    history = [dynF_a.ghid]
)
dynF_c = TEST_AGENT3.make_bind_dynamic(
    target = cont2_2.ghid,
    ghid_dynamic = dynF_a.ghid_dynamic,
    history = [dynF_a.ghid]
)

# Make some debindings
dyndebind1_1 = TEST_AGENT1.make_debind(
    target = dyn1_1b.ghid_dynamic
)
dyndebind2_1 = TEST_AGENT2.make_debind(
    target = dyn2_1b.ghid_dynamic
)

# Make some debindings
debind1_1 = TEST_AGENT1.make_debind(
    target = bind1_1.ghid
)
debind1_2 = TEST_AGENT1.make_debind(
    target = bind1_2.ghid
)

# Make some debindings
debind2_1 = TEST_AGENT2.make_debind(
    target = bind2_1.ghid
)
debind2_2 = TEST_AGENT2.make_debind(
    target = bind2_2.ghid
)
debindR_1 = TEST_AGENT2.make_debind(
    target = handshake1_1.ghid
)
debindR_2 = TEST_AGENT1.make_debind(
    target = handshake2_1.ghid
)

# Make some debindings from an unknown author
debind3_1 = TEST_AGENT3.make_debind(
    target = bind3_1.ghid
)
debind3_2 = TEST_AGENT3.make_debind(
    target = bind3_2.ghid
)

# And make some author-inconsistent debindings
debind1_F = TEST_AGENT2.make_debind(
    target = bind1_1.ghid
)
debind2_F = TEST_AGENT1.make_debind(
    target = bind2_1.ghid
)
debindR_F = TEST_AGENT1.make_debind(
    target = handshake1_1.ghid
)

# And make some bad-target debindings
debind1_TF = TEST_AGENT1.make_debind(
    target = cont1_1.ghid
)
debind3_TF = TEST_AGENT3.make_debind(
    target = cont3_1.ghid
)

# And then make some debindings for the debindings
dedebind1_1 = TEST_AGENT1.make_debind(
    target = debind1_1.ghid
)
dedebind1_2 = TEST_AGENT1.make_debind(
    target = debind1_2.ghid
)

# And then make some debindings for the debindings
dedebind2_1 = TEST_AGENT2.make_debind(
    target = debind2_1.ghid
)
dedebind2_2 = TEST_AGENT2.make_debind(
    target = debind2_2.ghid
)

# And then make some debindings for the debindings for the...
dededebind2_1 = TEST_AGENT2.make_debind(
    target = dedebind2_1.ghid
)
dededebind2_2 = TEST_AGENT2.make_debind(
    target = dedebind2_2.ghid
)
