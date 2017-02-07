# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

import base64
import uuid
import datetime

from decimal import Decimal
from isodate import duration_isoformat, parse_duration

import six

from gremlin_python.structure.io.graphson import GraphSONUtil

from gremlin_python.statics import IntType, LongType, long

from dse.graph import (
    Vertex as DseVertex,
    VertexProperty as DseVertexProperty,
    Edge as DseEdge,
    Path as DsePath
)
from dse_graph.predicates import GeoP, TextDistanceP
from dse.util import Point, LineString, Polygon, Distance

MAX_INT32 = 2**32-1

"""
This file is temporary and will be removed. A refactor is required in gremlin_python.

Supported types:

DSE Graph      GraphSON 2.0     Python Driver
------------ | -------------- | ------------
bigint       | g:Int64        | long
int          | g:Int32        | int
double       | g:Double       | float
float        | g:Float        | float
uuid         | g:UUID         | UUID
bigdecimal   | gx:BigDecimal  | Decimal
duration     | gx:Duration    | timedelta
inet         | gx:InetAddress | str (unicode)
timestamp    | gx:Instant     | Datetime
smallint     | gx:Int16       | int
varint       | gx:BigInteger  | long
date         | gx:LocalDate   | Date
polygon      | dse:Polygon    | Polygon
point        | dse:Point      | Point
linestring   | dse:LineString | LineString
blob         | dse:Blob       | bytearray, buffer (PY2), memoryview (PY3), bytes (PY3)
"""


class IntegerSerializer(object):
    @classmethod
    def dictify(cls, n, _):
        if six.PY3 and type(n) in six.integer_types and n > MAX_INT32:
            n = long(n)

        if isinstance(n, bool):  # because isinstance(False, int) and isinstance(True, int)
            return n
        elif isinstance(n, long):
            return GraphSONUtil.typedValue('Int64', n)
        else:
            return GraphSONUtil.typedValue('Int32', n)


class Int16Deserializer(object):
    @classmethod
    def objectify(cls, v, _):
        return v


class Int64Deserializer(object):
    @classmethod
    def objectify(cls, v, _):
        if six.PY3:
            return v
        return long(v)


class GeoPSerializer(object):
    @classmethod
    def dictify(cls, p, writer):
        out = {
            "predicateType": "Geo",
            "predicate": p.operator,
            "value": [writer.toDict(p.value), writer.toDict(p.other)] if p.other is not None else writer.toDict(p.value)
        }
        return GraphSONUtil.typedValue("P", out, prefix='dse')


class TextDistancePSerializer(object):
    @classmethod
    def dictify(cls, p, writer):
        out = {
            "predicate": p.operator,
            "value": {
                'query': writer.toDict(p.value),
                'distance': writer.toDict(p.distance)
            }
        }
        return GraphSONUtil.typedValue("P", out)


class UUIDIO(object):
    @classmethod
    def dictify(cls, v, _):
        return GraphSONUtil.typedValue('UUID', six.text_type(v))

    @classmethod
    def objectify(cls, v, _):
        return uuid.UUID(v)


class BigDecimalIO(object):
    @classmethod
    def dictify(cls, v, _):
        return GraphSONUtil.typedValue('BigDecimal', six.text_type(v), prefix='gx')

    @classmethod
    def objectify(cls, v, _):
        return Decimal(v)


class InstantIO(object):
    @classmethod
    def dictify(cls, v, _):
        if isinstance(v, datetime.datetime):
            v = datetime.datetime(*v.utctimetuple()[:7])
        else:
            v = datetime.datetime.combine(v, datetime.datetime.min.time())
        v = "{0}Z".format(v.isoformat())
        return GraphSONUtil.typedValue('Instant', v, prefix='gx')

    @classmethod
    def objectify(cls, v, _):
        try:
            d = datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            d = datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%SZ')
        return d


class DurationIO(object):
    @classmethod
    def dictify(cls, v, _):
        return GraphSONUtil.typedValue('Duration', duration_isoformat(v), prefix='gx')

    @classmethod
    def objectify(cls, v, _):
        return parse_duration(v)


class BlobIO(object):
    @classmethod
    def dictify(cls, v, _):
        v = base64.b64encode(v)
        if six.PY3:
            v = v.decode('utf-8')
        return GraphSONUtil.typedValue('Blob', v, prefix='dse')

    @classmethod
    def objectify(cls, v, _):
        v = base64.b64decode(v)
        return bytearray(v)


class PointIO(object):
    @classmethod
    def dictify(cls, v, _):
        return GraphSONUtil.typedValue('Point', six.text_type(v), prefix='dse')

    @classmethod
    def objectify(cls, v, _):
        return Point.from_wkt(v)


class LineStringIO(object):
    @classmethod
    def dictify(cls, v, _):
        return GraphSONUtil.typedValue('LineString', six.text_type(v), prefix='dse')

    @classmethod
    def objectify(cls, v, _):
        return LineString.from_wkt(v)


class PolygonIO(object):
    @classmethod
    def dictify(cls, v, _):
        return GraphSONUtil.typedValue('Polygon', six.text_type(v), prefix='dse')

    @classmethod
    def objectify(cls, v, _):
        return Polygon.from_wkt(v)


class DistanceIO(object):
    @classmethod
    def dictify(cls, v, _):
        return GraphSONUtil.typedValue('Distance', six.text_type(v), prefix='dse')

    @classmethod
    def objectify(cls, v, _):
        return Point.from_wkt(v)


class DateIO(object):
    FORMAT = '%Y-%m-%d'

    @classmethod
    def dictify(cls, v, _):
        return GraphSONUtil.typedValue('LocalDate', v.strftime(cls.FORMAT), prefix='gx')

    @classmethod
    def objectify(cls, v, _):
        try:
            return datetime.datetime.strptime(v, cls.FORMAT).date()
        except ValueError:
            # negative date
            return v


class StringDeserializer(object):
    @classmethod
    def objectify(cls, v, _):
        return six.text_type(v)


class DseVertexDeserializer(object):
    @classmethod
    def objectify(cls, v, reader):
        dse_vertex = DseVertex(reader.toObject(v["id"]), v["label"] if "label" in v else "vertex", 'vertex', {})
        dse_vertex.properties = reader.toObject(v.get('properties', {}))
        return dse_vertex


class DseVertexPropertyDeserializer(object):
    @classmethod
    def objectify(cls, v, reader):
        return DseVertexProperty(v['label'], reader.toObject(v["value"]), reader.toObject(v.get('properties', {})))


class DseEdgeDeserializer(object):
    @classmethod
    def objectify(cls, v, reader):
        return DseEdge(
            reader.toObject(v["id"]),
            v["label"] if "label" in v else "vertex",
            'edge', reader.toObject(v.get("properties", {})),
            DseVertex(reader.toObject(v["inV"]), v['inVLabel'], 'vertex', {}), v['inVLabel'],
            DseVertex(reader.toObject(v["outV"]), v['outVLabel'], 'vertex', {}), v['outVLabel']
        )


class DsePropertyDeserializer(object):
    @classmethod
    def objectify(cls, v, reader):
        return {v["key"], reader.toObject(v["value"])}


class DsePathDeserializer(object):
    @classmethod
    def objectify(cls, v, reader):
        labels = []
        objects = []
        for label in v["labels"]:
            labels.append(set(label))
        for object in v["objects"]:
            objects.append(reader.toObject(object))
        p = DsePath(labels, [])
        p.objects = objects
        return p


serializers = {
    LongType: IntegerSerializer,
    IntType: IntegerSerializer,
    uuid.UUID: UUIDIO,
    Decimal: BigDecimalIO,
    datetime.datetime: InstantIO,
    datetime.timedelta: DurationIO,
    datetime.date: DateIO,
    bytearray: BlobIO,
    Point: PointIO,
    LineString: LineStringIO,
    Polygon: PolygonIO,
    Distance: DistanceIO,
    GeoP: GeoPSerializer,
    TextDistanceP: TextDistancePSerializer
}

if six.PY2:
    serializers.update({
        buffer: BlobIO,
    })
else:
    serializers.update({
        memoryview: BlobIO,
        bytes: BlobIO,
    })

deserializers = {
    "gx:Int16": Int16Deserializer,
    "g:Int64": Int64Deserializer,
    "g:UUID": UUIDIO,
    "gx:BigInteger": Int64Deserializer,
    "gx:BigDecimal": BigDecimalIO,
    "gx:Instant": InstantIO,
    "gx:Duration": DurationIO,
    "gx:InetAddress": StringDeserializer,
    "dse:Blob": BlobIO,
    "dse:Point": PointIO,
    "dse:LineString": LineStringIO,
    "dse:Polygon": PolygonIO,
    "dse:Distance": DistanceIO,
    "gx:LocalDate": DateIO
}

dse_deserializers = deserializers.copy()
dse_deserializers.update({
    'g:Vertex': DseVertexDeserializer(),
    'g:VertexProperty': DseVertexPropertyDeserializer(),
    'g:Edge': DseEdgeDeserializer(),
    'g:Property': DsePropertyDeserializer(),
    'g:Path': DsePathDeserializer()
})
