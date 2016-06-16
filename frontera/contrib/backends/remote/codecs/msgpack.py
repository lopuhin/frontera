# -*- coding: utf-8 -*-
from __future__ import absolute_import

import six
from msgpack import packb, unpackb

from frontera.core.codec import BaseDecoder, BaseEncoder


_basic_types = six.string_types + six.integer_types + (float, bool)


def _prepare_request_message(request):
    def serialize(obj):
        """Recursively walk object's hierarchy."""
        if isinstance(obj, _basic_types):
            return obj
        elif isinstance(obj, dict):
            obj = obj.copy()
            for key in obj:
                obj[key] = serialize(obj[key])
            return obj
        elif isinstance(obj, list):
            return [serialize(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(serialize([item for item in obj]))
        elif hasattr(obj, '__dict__'):
            return serialize(obj.__dict__)
        else:
            return None
    return [request.url, request.headers, request.cookies, serialize(request.meta)]


def _prepare_response_message(response, send_body):
    return [response.url, response.status_code, response.meta, response.body if send_body else None]


class Encoder(BaseEncoder):
    def __init__(self, request_model, *a, **kw):
        self.send_body = True if 'send_body' in kw and kw['send_body'] else False

    def encode_add_seeds(self, seeds):
        return packb(['as', list(map(_prepare_request_message, seeds))])

    def encode_page_crawled(self, response, links):
        return packb(['pc', _prepare_response_message(response, self.send_body), list(map(_prepare_request_message, links))])

    def encode_request_error(self, request, error):
        return packb(['re', _prepare_request_message(request), str(error)])

    def encode_request(self, request):
        return packb(_prepare_request_message(request))

    def encode_update_score(self, fingerprint, score, url, schedule):
        return packb(['us', fingerprint, score, url, schedule])

    def encode_new_job_id(self, job_id):
        return packb(['njid', int(job_id)])

    def encode_offset(self, partition_id, offset):
        return packb(['of', int(partition_id), int(offset)])


class Decoder(BaseDecoder):
    def __init__(self, request_model, response_model, *a, **kw):
        self._request_model = request_model
        self._response_model = response_model

    def _response_from_object(self, obj):
        url, status_code, meta, body = obj
        url = url.decode('utf-8')
        meta = self._decode_meta(meta)
        return self._response_model(url=url,
                                    status_code=status_code,
                                    body=body,
                                    request=self._request_model(url=url,
                                                                meta=meta))

    def _request_from_object(self, obj):
        url, headers, cookies, meta = obj
        url = url.decode('utf-8')
        meta = self._decode_meta(meta)
        return self._request_model(url=url,
                                   headers=headers,
                                   cookies=cookies,
                                   meta=meta)

    def _decode_meta(self, meta):
        if six.PY2:
            return meta
        decoded = {}
        for k, v in meta.items():
            if isinstance(k, bytes):
                k = k.decode('utf-8')
            if isinstance(v, bytes):
                v = v.decode('utf-8')
            decoded[k] = v
        return decoded

    def decode(self, buffer):
        obj = unpackb(buffer)
        if obj[0] == b'pc':
            return ('page_crawled',
                    self._response_from_object(obj[1]),
                    list(map(self._request_from_object, obj[2])))
        if obj[0] == b'us':
            fp, score, url, flag = obj[1:]
            return ('update_score',
                    fp.decode('ascii'), score, url.decode('utf-8'), flag)
        if obj[0] == b're':
            request, error = obj[1:]
            error = error.decode('utf-8')
            return ('request_error', self._request_from_object(request), error)
        if obj[0] == b'as':
            return ('add_seeds', list(map(self._request_from_object, obj[1])))
        if obj[0] == b'njid':
            return ('new_job_id', int(obj[1]))
        if obj[0] == b'of':
            return ('offset', int(obj[1]), int(obj[2]))
        raise TypeError('Unknown message type')

    def decode_request(self, buffer):
        return self._request_from_object(unpackb(buffer))