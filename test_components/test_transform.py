import os

from components.transform import (
    Magnitude,
    ParseID,
    ToLowercaseColumns,)


def test_lowercase():
    component = ToLowercaseColumns()
    assert ['lolo1', 'lolo2'] == component._to_lowercase(['Lolo1', 'LoLo2'])


def test_extract_id():
    component = ParseID()
    assert 'bb' == component._extract_id('aa-aa-bb-aa-aa')


def test_magnitude():
    component = Magnitude()
    test_cases = [(600, 'massive'), (123, 'big'), (60, 'medium'),
                  (11, 'small'), (5, 'tiny'), (-1, 'unknown'),
                  (1001, 'unknown')]

    for size, magnitude in test_cases:
        assert magnitude == component._map_size(size)
