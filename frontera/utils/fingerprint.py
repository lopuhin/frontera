from __future__ import absolute_import
import hashlib
from six.moves.urllib.parse import urlparse
from struct import pack
from binascii import hexlify
from frontera.utils.misc import get_crc32
from frontera.utils.url import parse_url
from w3lib.util import to_native_str, to_bytes


def sha1(key):
    return hashlib.sha1(to_bytes(key, 'utf8')).hexdigest()


def md5(key):
    return hashlib.md5(to_bytes(key, 'utf8')).hexdigest()


def hostname_local_fingerprint(key):
    """
    This function is used for URL fingerprinting, which serves to uniquely identify the document in storage.
    ``hostname_local_fingerprint`` is constructing fingerprint getting first 4 bytes as Crc32 from host, and rest is MD5
    from rest of the URL. Default option is set to make use of HBase block cache. It is expected to fit all the documents
    of average website within one cache block, which can be efficiently read from disk once.

    :param key: str URL
    :return: str 20 bytes hex string
    """
    result = parse_url(key)
    if not result.hostname:
        return sha1(key)
    host_checksum = get_crc32(result.hostname)
    doc_uri_combined = result.path+';'+result.params+result.query+result.fragment

    doc_uri_combined = to_bytes(doc_uri_combined, 'utf8', 'ignore')
    doc_fprint = hashlib.md5(doc_uri_combined).digest()
    fprint = hexlify(pack(">i16s", host_checksum, doc_fprint))
    return to_native_str(fprint, 'utf8')
