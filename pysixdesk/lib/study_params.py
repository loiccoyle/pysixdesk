import re
import os
import logging
from collections import OrderedDict

from .constants import PROTON_MASS
from .utils import PYSIXDESK_ABSPATH


class FixedDict(OrderedDict):
    '''
    OrderedDict but with locking mechanism which stops user from entering new
    keys
    '''

    def __init__(self, *args, **kwargs):
        self._locked = False
        super().__init__(*args, **kwargs)
        self._locked = True

    def lock(self):
        self._locked = True

    def unlock(self):
        self._locked = False

    def __setitem__(self, key, value):
        if self._locked:
            if key in self.keys():
                super().__setitem__(key, value)
            else:
                raise ValueError(f'Dictionnary locked, cannot add "{key}" key.')
        else:
            super().__setitem__(key, value)


class StudyParams:
    '''
    Looks for any placeholders in the provided paths and extracts the
    placeholder if no default values found, use None.
    Unifies all the placeholders in self.placeholders, the user can edit it's
    values, but not it's keys. This implements the __setitem__ and __getitem__
    so the user can interact with the StudyParams object similarly to a dict.

    The placeholders are split up again, into oneturn, sixtrack and
    and mask dicts for use in pysixdesk.
    To get the placeholder patterns for the mask file use self.madx_params.
    To get the placeholder patterns for the oneturn sixtrack job use
    self.oneturn_params.
    To get the placeholder patterns for the sixtrack file use
    self.sixtrack_params.
    '''

    def __init__(self, mask_path, fort_path=f'{PYSIXDESK_ABSPATH}/templates/fort.3'):
        self._logger = logging.getLogger(__name__)
        # comment regexp
        self._reg_comment = re.compile(r'^(\s?!|\s?/).*', re.MULTILINE)
        # placeholder pattern regexp
        self._reg = re.compile(r'%(?!FILE|%)([a-zA-Z0-9_]+/?)')
        self.fort_path = fort_path
        self.mask_path = mask_path
        # initialize empty calculation queue
        self.calc_queue = []
        # default parameters for the sixtrack job
        self.defaults = dict([
                             ("ax0s", 0.1),
                             ("ax1s", 0.1),
                             ("BSEP", 25),
                             ("bunch_charge", 1.15E11),  # 2.2e11, 3.5e11 ?
                             ("chromx", 2),  # 3.0?
                             ("chromy", 2),   # 3.0?
                             ("chrom_eps", 0.000001),
                             ("dp1", 0.000001),
                             ("dp2", 0.000001),
                             ("e0", 7000),
                             ("emit_beam", 3.75),
                             ("turnss", 1e4),
                             ("ibtype", 0),
                             ("iclo6", 2),
                             ("idfor", 0),
                             ("imc", 1),
                             ("inttunex", 62),
                             ("inttuney", 60),
                             ("IOCT", -300),
                             ("ition", 0),
                             ("length", 26658.864),
                             ("ndafi", 1),
                             ("nss", 60),  # I think this should be 60?
                             ("pmass", PROTON_MASS),
                             ("PHI_IR5", 0),
                             ("PHI_IR1", 90.0),
                             ("Runnam", 'FirstTurn'),
                             ("rfvol", 16),
                             ("ratios", 1),
                             ("tunex", 62.28),
                             ("tuney", 60.31),
                             ("toggle_post/", '/'),
                             ("toggle_diff/", '/'),
                             ("writebins", 1),
                             ("XING", 255),
                             ])
        # phasespace params
        amp = [8, 10, 12]  # The amplitude
        self.phasespace_params = dict([
                                      ('amp', list(zip(amp, amp[1:]))),
                                      ('kang', list(range(1, 1 + 1))),
                                      ('kmax', 5),
                                      ])

        self.madx_params = FixedDict(**self.find_patterns(self.mask_path))
        self.sixtrack_params = FixedDict(**self.find_patterns(self.fort_path))

    def extract_patterns(self, file):
        '''
        Extracts the patterns from a file.
        '''
        with open(file) as f:
            lines = f.read()
        lines_no_comments = re.sub(self._reg_comment, '', lines)
        matches = re.findall(self._reg, lines_no_comments)
        return matches

    def find_patterns(self, file_path, folder=False):
        '''
        Reads file_path and populates a dict with the matched patterns and
        values

        :param folder: if True, check also the files in the same folder as
        the mask file for placeholder patterns.
        '''
        dirname = os.path.dirname(file_path)
        if folder and dirname != '':
            # extract the patterns for all the files in the directory of the
            # maskfile
            matches = []
            for file in os.listdir(dirname):
                matches += self.extract_patterns(os.path.join(dirname,
                                                              file))
        else:
            matches = self.extract_patterns(file_path)

        out = OrderedDict()
        for ph in matches:
            if ph in self.defaults.keys():
                out[ph] = self.defaults[ph]
            else:
                out[ph] = None

        self._logger.debug(f'Found {len(matches)} placeholders.')
        self._logger.debug(f'With {len(set(matches))} unique placeholders.')
        for k, v in out.items():
            self._logger.debug(f'{k}: {v}')
        return out

    @property
    def oneturn_params(self):
        sixtrack = self.sixtrack_params.copy()
        sixtrack['turnss'] = 1
        sixtrack['nss'] = 1
        sixtrack['Runnam'] = 'FirstTurn'
        return sixtrack

    def add_calc(self, in_keys, out_key, fun):
        '''
        Add calculations to the calc queue. Any extra arguments of
        fun can be given in the *args/**kwargs of the self.calc call.
        '''
        if isinstance(in_keys, list):
            self.calc_queue.append([in_keys, out_key, fun])
        else:
            self.calc_queue.append([[in_keys], out_key, fun])

    def calc(self, *args, **kwargs):
        '''
        Runs the queued calculations, in order.
        *args and **kwargs are passed to the queued function
        at run time.
        '''
        for in_keys, out_key, fun in self.calc_queue:
            # get the input values with __getitem__
            inp = [self.__getitem__(k) for k in in_keys]

            if isinstance(out_key, list):
                out = fun(*inp, *args, **kwargs)
                for i, k in enumerate(out_key):
                    self.sixtrack_params[k] = out[i]
        return self.sixtrack_params

    def __repr__(self):
        return '/n'.join([self.madx_params.__repr__(),
                          self.sixtrack_params.__repr__(),
                          self.phasespace_params.__repr__()])

    # set and get items like a dict
    def __setitem__(self, key, val):
        '''
        Adds entry to the appropriate dictionnary.
        '''
        if key in self.phasespace_params.keys():
            self.phasespace_params[key] = val
        if key in self.madx_params.keys():
            self.madx_params[key] = val
        if key in self.sixtrack_params.keys():
            self.sixtrack_params[key] = val
        else:
            raise KeyError(f'"{key}" not in extracted placeholders.')

    def __getitem__(self, key):
        '''
        Gets entry from the appropriate dictionnary.
        '''
        if key in self.phasespace_params.keys():
            return self.phasespace_params[key]
        if key in self.madx_params.keys():
            return self.madx_params[key]
        if key in self.sixtrack_params.keys():
            return self.sixtrack_params[key]

    def update(self, *args, **kwargs):
        '''
        Updates both dictionnaries.
        '''
        self.madx_params.update(*args, **kwargs)
        self.sixtrack_params.update(*args, **kwargs)


# for testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    test = StudyParams('../templates/lhc_aperture/hl13B1_elens_aper.mask')
    print(test.placeholders)
