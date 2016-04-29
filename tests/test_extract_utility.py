
# pylint extract_utility.py
# coverage run --include extract/extract_utility.py -m py.test tests/test_extract_utility.py
# coverage report -m

import sys
import os
import pytest

sys.path.append( os.path.join( os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ), 'src' ) )

from extract_utility import *

DATA = os.path.join( os.path.dirname( os.path.abspath(__file__) ), 'data' )

exo = ExtractObject()



def test_set_vector_path():

    # no extract method set
    exo._extract_method = None
    with pytest.raises(Exception):
        exo.set_vector_path('test.test')

    # invalid file path
    exo._extract_method = "python"
    with pytest.raises(Exception):
        exo.set_vector_path('test.test')

    # valid file path and invalid extenions
    # exo._set_vector_path(os.path.join(DATA, 'test.txt')

    # valid file path and extension
    # exo._set_vector_path(os.path.join(DATA, 'boundary/test.geojson')



def test_set_vector_extension():
    # basic functionality for valid extensions
    for i in exo._vector_extensions:
        exo._set_vector_extension('test' + i)

    # bad extension
    with pytest.raises(Exception):
        exo._set_vector_extension('test.test')


def test_check_file_mask():

    exo._base_path = None
    exo._file_mask = None
    exo._check_file_mask()

    exo._base_path = "test.tif"
    exo._file_mask = None
    exo._check_file_mask()

    exo._base_path = None
    exo._file_mask = "None"
    exo._check_file_mask()

    exo._base_path = "/path/to/test.tif"
    exo._file_mask = "None"
    exo._check_file_mask()
    exo._file_mask = "YYYY"
    with pytest.raises(Exception):
        exo._check_file_mask()

    exo._base_path = "/path/to/test"
    exo._file_mask = "YYYY"
    exo._check_file_mask()
    exo._file_mask = "None"
    with pytest.raises(Exception):
        exo._check_file_mask()


def test_set_base_path():
    exo._file_mask = None
    exo.set_base_path(DATA)
    assert exo._base_path == DATA

    with pytest.raises(Exception):
        exo.set_base_path("/fake/absolute/path")
    with pytest.raises(Exception):
        exo.set_base_path("fake/relative/path")


def test_str_to_range():
    assert str_to_range("1900:1905") == range(1900,1905+1)
    assert str_to_range("1905:1900") == range(1905,1900)
    with pytest.raises(Exception):
        str_to_range("1905:x")
    with pytest.raises(Exception):
        str_to_range("x")
    with pytest.raises(Exception):
        str_to_range(1900)
    with pytest.raises(Exception):
        str_to_range("1900")
    with pytest.raises(Exception):
        str_to_range("1900:1905:1910")


def test_set_years():

    exo.set_years("")
    # assert exo._years == exo.default_years
    assert exo._years == map(str, range(1000,10000))

    exo.set_years("1900")
    assert exo._years == map(str, [1900])

    exo.set_years("!1900")
    assert exo._years == []

    exo.set_years("1900:1910")
    assert exo._years == map(str, range(1900,1910+1))

    exo.set_years("1900:1910")
    assert exo._years == map(str, range(1900,1910+1))

    exo.set_years("1900:1910|!1905")
    assert exo._years == map(str, [1900,1901,1902,1903,1904,1906,1907,1908,1909,1910])

    exo.set_years("1900:1910|!1905:1910|1950")
    assert exo._years == map(str, range(1900,1905) + [1950])

    exo.set_years("!1900:1910|!1905|1900:1910")
    assert exo._years == map(str, range(1900,1910+1))

    with pytest.raises(Exception):
        exo.set_years("1900:1910|x")
    with pytest.raises(Exception):
        exo.set_years("x")
    with pytest.raises(Exception):
        exo.set_years(range(1900,1910))


def test_set_file_mask():

    exo._base_path = None
    exo.set_file_mask("None")
    exo.set_file_mask("YYYY")
    exo.set_file_mask("YYYY_MM")
    exo.set_file_mask("YYYY_DDD")
    with pytest.raises(Exception):
        exo.set_file_mask("YYYY_MM_DDD")
    with pytest.raises(Exception):
        exo.set_file_mask("YYYY_DDD_MM")
    with pytest.raises(Exception):
        exo.set_file_mask("test")
    with pytest.raises(Exception):
        exo.set_file_mask("YYY")
    with pytest.raises(Exception):
        exo.set_file_mask("DD")
    with pytest.raises(Exception):
        exo.set_file_mask("MM_DDD")

    exo._base_path = "test.tif"
    exo.set_file_mask("None")
    with pytest.raises(Exception):
        exo.set_file_mask("YYYY")

    exo._base_path = "test"
    exo.set_file_mask("YYYY")
    with pytest.raises(Exception):
        exo.set_file_mask("None")


def test_extract_type():
    for i in exo._extract_options.keys():
        exo.set_extract_type(i)
        assert exo._extract_type == i

    with pytest.raises(Exception):
        exo.set_extract_type("test")
    with pytest.raises(Exception):
        exo.set_extract_type(1)


# def test_gen_data_list():
    # something


# def test_run_extract():
    # something


# def run_tests():
#     test_set_extract_method()
#     test_set_vector_path()
#     test_set_vector_extension()
#     test_check_file_mask()
#     test_set_base_path()
#     test_str_to_range()
#     test_set_years()
#     test_set_file_mask()
#     test_extract_type()


# run_tests()
