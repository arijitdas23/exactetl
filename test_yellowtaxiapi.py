from unittest import TestCase
from yellowtaxiapi import *
# Test cases for the API's
class TestYellowTaxiApi(TestCase):
    def test_get_max_tip_percentage(self):
        ret_json_str,status = getMaxTipPercentage('2020','1')
        if status==1:
            self.fail()
        else:
            assert status == 0
    def test_get_max_tip_percentage_nonnumeric(self):
        ret_json_str, status = getMaxTipPercentage('2020', '1bc')
        assert status == 1

    def test_get_max_trip_speed(self):
        ret_json_str, status = getMaxTripSpeed('2020', '1',None)
        if status == 1:
            self.fail()
        else:
            assert status == 0
