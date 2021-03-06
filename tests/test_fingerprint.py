# -*- coding: utf-8 -*-
from __future__ import absolute_import
from frontera.utils.fingerprint import hostname_local_fingerprint, sha1, md5
from w3lib.util import to_bytes


url1 = u"https://news.yandex.ru/yandsearch?cl4url=top.rbc.ru/politics/14/07/2015/55a50b509a79473f583e104c&lang=ru&lr=54#fragment"
url2 = u"TestString"
url3 = u"http://www.example.com/some/page\u5000/"


class TestFingerprint(object):

    def test_sha1_bytes(self):
        assert sha1(to_bytes(url1)) == '880c5e7919cb09e182bd639d724bce6d90db71eb'
        assert sha1(to_bytes(url2)) == 'd598b03bee8866ae03b54cb6912efdfef107fd6d'
        assert sha1(to_bytes(url3)) == '28bf812b6421a46ee5bcf40c05a82e8f051ab88e'

    def test_sha1_unicode(self):
        assert sha1(url1) == '880c5e7919cb09e182bd639d724bce6d90db71eb'
        assert sha1(url2) == 'd598b03bee8866ae03b54cb6912efdfef107fd6d'
        assert sha1(url3) == '28bf812b6421a46ee5bcf40c05a82e8f051ab88e'

    def test_md5_bytes(self):
        assert md5(to_bytes(url1)) == 'bb82110ce034c1a6ad55a2e73adc322a'
        assert md5(to_bytes(url2)) == '5b56f40f8828701f97fa4511ddcd25fb'
        assert md5(to_bytes(url3)) == '5abf5c9aa02d870756032bdec0bd6522'

    def test_md5_unicode(self):
        assert md5(url1) == 'bb82110ce034c1a6ad55a2e73adc322a'
        assert md5(url2) == '5b56f40f8828701f97fa4511ddcd25fb'
        assert md5(url3) == '5abf5c9aa02d870756032bdec0bd6522'

    def test_local_hostname_fingerprint_bytes(self):
        assert hostname_local_fingerprint(to_bytes(url1)) == '1be68ff556fd0bbe5802d1a100850da29f7f15b1'
        assert hostname_local_fingerprint(to_bytes(url2)) == 'd598b03bee8866ae03b54cb6912efdfef107fd6d'
        assert hostname_local_fingerprint(to_bytes(url3)) == '2ed642bbdf514b8520ab28f5da589ab28eda10a6'

    def test_local_hostname_frongerprint_unicode(self):
        assert hostname_local_fingerprint(url1) == '1be68ff556fd0bbe5802d1a100850da29f7f15b1'
        assert hostname_local_fingerprint(url2) == 'd598b03bee8866ae03b54cb6912efdfef107fd6d'
        assert hostname_local_fingerprint(url3) == '2ed642bbdf514b8520ab28f5da589ab28eda10a6'
