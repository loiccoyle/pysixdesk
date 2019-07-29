import re
import os
import logging
from collections import OrderedDict

from .constants import PROTON_MASS
from .utils import PYSIXDESK_ABSPATH


class StudyParams:
    '''
    Looks for any placeholders in the provided paths and extracts the
    placeholder if no default values found, use None.
    This implements the __setitem__ and __getitem__
    so the user can interact with the StudyParams object similarly to a dict.

    To get the placeholder patterns for the mask file use self.madx_params.
    To get the placeholder patterns for the oneturn sixtrack job use
    self.oneturn_params.
    To get the placeholder patterns for the sixtrack file use
    self.sixtrack_params.
    '''

    def __init__(self, mask_path, fort_path=f'{PYSIXDESK_ABSPATH}/templates/fort.3'):
        """
        Args:
            mask_path (str): path to the mask file
            fort_path (str): path to the fort file
        """
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
                             ("SEEDRAN", 1),
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

        self.madx_params = self.find_patterns(self.mask_path)
        self.sixtrack_params = self.find_patterns(self.fort_path)

    @property
    def oneturn_params(self):
        sixtrack = self.sixtrack_params.copy()
        sixtrack['turnss'] = 1
        sixtrack['nss'] = 1
        sixtrack['Runnam'] = 'FirstTurn'
        return sixtrack

    @property
    def unified_keys(self):
        return (list(self.madx_params.keys()) +
                list(self.sixtrack_params.keys()) +
                list(self.phasespace_params.keys()))

    def extract_patterns(self, file):
        '''
        Extracts the patterns from a file.

        Args:
            file (str): path to the file from which to extract the placeholder
            patterns.
        Returns:
            list: list containing the matches
        '''
        with open(file) as f:
            lines = f.read()
        lines_no_comments = re.sub(self._reg_comment, '', lines)
        matches = re.findall(self._reg, lines_no_comments)
        return matches

    def find_patterns(self, file_path, folder=False, keep_none=False):
        '''
        Reads file at `file_path` and populates a dict with the matched
        patterns and values taken from `self.defaults`.

        Args:
            file_path (str): path to file to extract placeholder patterns
            folder (bool, optional): if True, check for placeholder patterns
            in all files in the `file_path` fodler.
            keep_none (bool, optional): if True, keeps the None entries in the
            output dict.

        Returns:
            OrderedDict: dictionnary of the extracted placeholder patterns with
            their values set the entry on `self.defaults`.
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
            elif keep_none:
                out[ph] = None

        self._logger.debug(f'Found {len(matches)} placeholders.')
        self._logger.debug(f'With {len(set(matches))} unique placeholders.')
        for k, v in out.items():
            self._logger.debug(f'{k}: {v}')
        return out

    def add_calc(self, in_keys, out_key, fun):
        '''
        Add calculations to the calc queue. Any extra arguments of
        fun can be given in the *args/**kwargs of the self.calc call.

        Args:
            in_keys (list): keys to the input data of `fun`.
            out_key (list): keys to place the output `fun` in
            `self.sixtrack_params`. The `len(out_key)` must match the number of
            outputs of `fun`.
            fun (function): function to run, must take as input the values
            given by the `in_keys` and outputs to the `out_key` in
            `self.sitrack_params`. Can also have *args/**kwargs which will
            passed to it when calling `self.calc`
        '''

        self.calc_queue.append([in_keys, out_key, fun])

    def calc(self, *args, **kwargs):
        '''
        Runs the queued calculations, in order. *args and **kwargs are passed
        to the queued function at run time. The output of the queue is put
        in `self.sixtrack_params`.

        Args:
            *args: passed to the `fun` in the queued calculations
            **kwargs: passed to the `fun` in the queued calculations

        Returns:
            OrderedDict: `self.sixtrack_params` after running the calculation
            queue
        '''
        for in_keys, out_key, fun in self.calc_queue:
            # get the input values with __getitem__
            inp = [self.__getitem__(k) for k in in_keys]
            print(inp)

            out = fun(*inp, *args, **kwargs)
            for i, k in enumerate(out_key):
                self.sixtrack_params[k] = out[i]
        return self.sixtrack_params

    def __repr__(self):
        '''
        Unified __repr__ of the three dictionnaries.
        '''
        return '\n\n'.join(['Madx params: ' + self.madx_params.__repr__(),
                            'SixTrack params: ' + self.sixtrack_params.__repr__(),
                            'Phase space params: ' + self.phasespace_params.__repr__()])

    # set and get items like a dict
    def __setitem__(self, key, val):
        '''
        Adds entry to the appropriate dictionnary(ies) which already contains
        the key.
        '''
        if key not in self.unified_keys:
            raise KeyError(f'"{key}" not in extracted placeholders.')
        if key in self.phasespace_params.keys():
            self.phasespace_params[key] = val
        if key in self.madx_params.keys():
            self.madx_params[key] = val
        if key in self.sixtrack_params.keys():
            self.sixtrack_params[key] = val

    def __getitem__(self, key):
        '''
        Gets entry from the dictionnary which contains the key.
        '''
        if key not in self.unified_keys:
            raise KeyError(key)
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
    print(test.params)
