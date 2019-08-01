import unittest
import shutil
from pathlib import Path
import sys
# give the test runner the import access
sys.path.insert(0, Path(__file__).parents[1].absolute())
from pysixdesk.lib import utils


class UtilsTest(unittest.TestCase):

    def setUp(self):
        self.str_list = ['workspace/studies/study', 'pysixdesk/templates']
        self.str_list_out = 'workspace/studies/study,pysixdesk/templates'

        self.str_dic = {'fc.2': 'fort.2',
                        'fc.3': 'fort.3.mad',
                        'fc.3.aux': 'fort.3.aux',
                        'fc.8': 'fort.8',
                        'fc.16': 'fort.16',
                        'fc.34': 'fort.34'}
        self.str_dic_out = 'fc.2:fort.2,fc.3:fort.3.mad,fc.3.aux:fort.3.aux,fc.8:fort.8,fc.16:fort.16,fc.34:fort.34'

        # prepare a testing folder
        self.test_folder = Path('unit_test/utils/')
        self.test_folder.mkdir(parents=True, exist_ok=True)

        # replacement file
        self.replace_file_in = self.test_folder / 'replace_test.in'
        self.replace_file_out = self.test_folder / 'replace_test.out'
        self.contents = ['%pattern1', 'var = %pattern2;', 'var=%pattern3, var;']
        self.patterns = ['%pattern1', '%pattern2', '%pattern3']
        self.replace = [1, 2.5, 1e11]
        with open(self.replace_file_in, 'w') as f:
            f.writelines('\n'.join(self.contents))

    def test_encode_strings(self):
        self.assertEqual(utils.encode_strings(self.str_list),
                         (True, self.str_list_out))

        self.assertEqual(utils.encode_strings(self.str_dic),
                         (True, self.str_dic_out))
        # Expected failure case
        self.assertEqual(utils.encode_strings(10),
                         (False, ''))

    def test_decode_strings(self):
        self.assertEqual(utils.decode_strings(self.str_list_out),
                         (True, self.str_list))

        self.assertEqual(utils.decode_strings(self.str_dic_out),
                         (True, self.str_dic))
        # Expected failure case
        self.assertEqual(utils.decode_strings(10),
                         (False, []))

    def test_replace(self):
        utils.replace(self.patterns,
                      self.replace,
                      self.replace_file_in,
                      self.replace_file_out)
        with open(self.replace_file_out, 'r') as f:
            out = f.readlines()
        out = [l.rstrip() for l in out]
        self.assertEqual(out, ['1', 'var = 2.5;', 'var=100000000000.0, var;'])

    def test_compress_buf(self):
        # with strings
        in_str = 'qwertyuiopasdfghjklzxcvbnm_-./'
        _, in_str_comp = utils.compress_buf(in_str, source='str')
        _, in_str_decomp = utils.decompress_buf(in_str_comp, None, des='buf')
        self.assertEqual(in_str, in_str_decomp)
        # with file ...

        # with gzip ...

    def test_merge_dicts(self):
        a = {'a': 1, 'b': 2, 'c': 3}
        b = {'a': 10}
        self.assertEqual(utils.merge_dicts(a, b), {'a': 10, 'b': 2, 'c': 3})
        # also check to make sure the dicts have not been changed
        self.assertEqual(a, {'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(b, {'a': 10})

        b = {'d': 10}
        self.assertEqual(utils.merge_dicts(a, b), {'a': 1, 'b': 2,
                                                   'c': 3, 'd': 10})
        self.assertEqual(a, {'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(b, {'d': 10})

    def tearDown(self):
        # remove testing folder
        shutil.rmtree(self.test_folder.parents[0], ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
