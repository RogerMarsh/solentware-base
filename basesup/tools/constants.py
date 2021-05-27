# constants.py
# Copyright (c) 2009 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Constants defined for convenience in using Tkinter.

"""

# state values in keyboard events.
# These value mappings may vary by system and keyboard (Tk reference forgotten).
# (CapsLock and NumLk are key labels; presume one is Lock key).
# Combinations are allowed: 5 is Shift down and Control down and so on.

SHIFTDOWN = 1
CAPSLOCKDOWN = 2
CONTROLDOWN = 4
ALTDOWN = 8
NUMLOCKDOWN = 16
START_MSWINDOWS = 64
