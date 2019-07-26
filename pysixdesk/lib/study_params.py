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
    To get the placeholder patterns for the onturn sixtrack file use
    self.oneturn_params.
    To get the placeholder patterns for the sixtrack file use
    self.sixtrack_params.
    '''

    def __init__(self, mask_path, fort_path=f'{PYSIXDESK_ABSPATH}/templates/fort.3'):
        self._logger = logging.getLogger(__name__)
        self._reg_comment = re.compile(r'^(\s?!|\s?/).*', re.MULTILINE)
        self._reg = re.compile(r'%(?!FILE|%)([a-zA-Z0-9_]+/?)')
        self.fort_path = fort_path
        self.mask_path = mask_path
        self.calc_queue = []
        # default parameters for the sixtack job
        self.defaults = dict([
                             ("turnss", 1e4),
                             ("nss", 60),  # I think this should be 60?
                             ("ax0s", 0.1),
                             ("ax1s", 0.1),
                             ("imc", 1),
                             ("iclo6", 2),
                             ("writebins", 1),
                             ("ratios", 1),
                             ("Runnam", 'FirstTurn'),
                             ("idfor", 0),
                             ("ibtype", 0),
                             ("ition", 0),
                             ("ndafi", 1),
                             ("tunex", 62.28),
                             ("tuney", 60.31),
                             ("inttunex", 62),
                             ("inttuney", 60),
                             ("toggle_post/", '/'),
                             ("toggle_diff/", '/'),
                             ("pmass", PROTON_MASS),
                             ("emit_beam", 3.75),
                             ("e0", 7000),
                             ("bunch_charge", 1.15E11),  # 2.2e11, 3.5e11 ?
                             ("CHROM", 0),
                             ("chrom_eps", 0.000001),
                             ("dp1", 0.000001),
                             ("dp2", 0.000001),
                             ("chromx", 2),  # 3.0?
                             ("chromy", 2),   # 3.0?
                             ("length", 26658.864),
                             ("XING", 255),
                             ("rfvol", 16),
                             ("PHI_IR5", 0),
                             ("PHI_IR1", 90.0),
                             ("IOCT", -300),
                             ("BSEP", 25),
                             ])

        amp = [8, 10, 12]  # The amplitude
        self.phasespace_params = dict([
                                      ('amp', list(zip(amp, amp[1:]))),
                                      ('kang', list(range(1, 1 + 1))),
                                      ('kmax', 5),
                                      ])

        self._mask_ph = self.find_patterns(self.mask_path)
        self._fort_ph = self.find_patterns(self.fort_path)
        self.placeholders = FixedDict(**self.merge())

        self._madx_params = None
        self._oneturn_params = None
        self._sixtrack_params = None

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

    def merge(self):
        '''
        Merges the mask and fort placeholders into a single dict.
        '''
        merged = OrderedDict()
        for k, v in self._mask_ph.items():
            merged[k] = v
        for k, v in self._fort_ph.items():
            if k not in merged.keys():
                merged[k] = v
        return merged

    @property
    def files(self):
        '''
        Creates an OrderedDict with the files in which each pattern occurs
        '''
        out = OrderedDict()
        for k in self.placeholders.keys():
            file = []
            if k in self._mask_ph.keys():
                file.append(self.mask_path)
            if k in self._fort_ph.keys():
                file.append(self.fort_path)
            out[k] = file
        return out

    def _filter_file(self, file, filter_none=True):
        '''
        Selects for the placeholders occuring in file.
        '''
        filtered = OrderedDict()
        for (k, v), f in zip(self.placeholders.items(), self.files.values()):
            if filter_none and v is None:
                continue
            if file in f:
                filtered[k] = v
        return filtered

    @property
    def oneturn_params(self):
        if self._oneturn_params is None:
            sixtrack = self._filter_file(self.fort_path)
            sixtrack['turnss'] = 1
            sixtrack['nss'] = 1
            sixtrack['Runnam'] = 'FirstTurn'
            self._oneturn_params = sixtrack
        return self._oneturn_params

    @property
    def sixtrack_params(self):
        if self._sixtrack_params is None:
            sixtrack = self._filter_file(self.fort_path)
            # This is needed for compatibility with the way the pre_calc is done.
            # it would be good to figure out a better to do it.
            sixtrack.update(self.phasespace_params)
            self._sixtrack_params = sixtrack
        return self._sixtrack_params

    @property
    def madx_params(self):
        if self._madx_params is None:
            madx = self._filter_file(self.mask_path)
            self._madx_params = madx
        return self._madx_params

    def add_calc(self, in_keys, out_key, fun):
        '''
        Add calculations to the calc queue. Any extra arguments of
        fun can be given in the *args/**kwargs of self.calc.
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

            inp = [self.sixtrack_params[k] for k in in_keys]

            if isinstance(out_key, list):
                out = fun(*inp, *args, **kwargs)
                for i, k in enumerate(out_key):
                    self.sixtrack_params[k] = out[i]
        return self.sixtrack_params

    # set and get items like a dict
    def __repr__(self):
        return self.placeholders.__repr__()

    def __setitem__(self, key, val):
        if key in self.phasespace_params.keys():
            self.phasespace_params[key] = val
        else:
            self.placeholders.__setitem__(key, val)

    def __getitem__(self, key):
        return self.placeholders.__getitem__(key)

    def update(self, *args, **kwargs):
        self.placeholders.update(*args, **kwargs)

# for testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    test = StudyParams('../templates/lhc_aperture/hl13B1_elens_aper.mask')
    print(test.placeholders)
