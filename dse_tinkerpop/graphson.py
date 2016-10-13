'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
'''

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

import base64
import json
import six
import uuid
import datetime
from abc import abstractmethod
from decimal import Decimal
from aenum import Enum
from isodate import duration_isoformat, parse_duration
from types import FloatType
from types import FunctionType
from types import IntType
from types import LongType

from gremlin_python import statics
from gremlin_python.process.traversal import Binding
from gremlin_python.process.traversal import Bytecode
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Traversal
from gremlin_python.process.traversal import Traverser
from gremlin_python.structure.graph import Edge
from gremlin_python.structure.graph import Property
from gremlin_python.structure.graph import Vertex
from gremlin_python.structure.graph import VertexProperty
#from gremlin_python.structure.graph import Path

from dse.util import Point, LineString, Polygon

"""
This file is temporary and will be removed. A refactor is required in gremlin_python.

Supported types:

DSE Graph       GraphSON 2.0    Python Driver
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
polygon      | dse:Polygon    | Polygon
point        | dse:Point      | Point
linestring   | dse:LineString | LineString
blob         | dse:Blob       | bytearray, buffer (PY2), memoryview (PY3), bytes (PY3)
"""


class GraphSONWriter(object):
    @staticmethod
    def _dictify(object):
        for key in serializers:
            if isinstance(object, key):
                return serializers[key]._dictify(object)
        # list and map are treated as normal json objects (could be isolated serializers)
        if isinstance(object, list):
            newList = []
            for item in object:
                newList.append(GraphSONWriter._dictify(item))
            return newList
        elif isinstance(object, dict):
            newDict = {}
            for key in object:
                newDict[GraphSONWriter._dictify(key)] = GraphSONWriter._dictify(object[key])
            return newDict
        else:
            return object

    @staticmethod
    def writeObject(objectData):
        return json.dumps(GraphSONWriter._dictify(objectData), separators=(',', ':'))


class GraphSONReader(object):
    @staticmethod
    def _objectify(object):
        if isinstance(object, dict):
            if _SymbolHelper._TYPE in object:
                type = object[_SymbolHelper._TYPE]
                if type in deserializers:
                    return deserializers[type]._objectify(object)
                    # list and map are treated as normal json objects (could be isolated deserializers)
            newDict = {}
            for key in object:
                newDict[GraphSONReader._objectify(key)] = GraphSONReader._objectify(object[key])
            return newDict
        elif isinstance(object, list):
            newList = []
            for item in object:
                newList.append(GraphSONReader._objectify(item))
            return newList
        else:
            return object

    @staticmethod
    def readObject(jsonData):
        return GraphSONReader._objectify(json.loads(jsonData))


'''
SERIALIZERS
'''


class GraphSONSerializer(object):
    @abstractmethod
    def _dictify(self, object):
        return object


class BytecodeSerializer(GraphSONSerializer):
    def _dictify(self, bytecode):
        if isinstance(bytecode, Traversal):
            bytecode = bytecode.bytecode
        dict = {}
        sources = []
        for instruction in bytecode.source_instructions:
            inst = []
            inst.append(instruction[0])
            for arg in instruction[1:]:
                inst.append(GraphSONWriter._dictify(arg))
            sources.append(inst)
        steps = []
        for instruction in bytecode.step_instructions:
            inst = []
            inst.append(instruction[0])
            for arg in instruction[1:]:
                inst.append(GraphSONWriter._dictify(arg))
            steps.append(inst)
        if len(sources) > 0:
            dict["source"] = sources
        if len(steps) > 0:
            dict["step"] = steps
        return _SymbolHelper.objectify("Bytecode", dict)


class TraverserSerializer(GraphSONSerializer):
    def _dictify(self, traverser):
        return _SymbolHelper.objectify("Traverser", {"value": GraphSONWriter._dictify(traverser.object),
                                                     "bulk": GraphSONWriter._dictify(traverser.bulk)})


class EnumSerializer(GraphSONSerializer):
    def _dictify(self, enum):
        return _SymbolHelper.objectify(_SymbolHelper.toGremlin(type(enum).__name__),
                                       _SymbolHelper.toGremlin(str(enum.name)))


class PSerializer(GraphSONSerializer):
    def _dictify(self, p):
        dict = {}
        dict["predicate"] = p.operator
        if p.other is None:
            dict["value"] = GraphSONWriter._dictify(p.value)
        else:
            dict["value"] = [GraphSONWriter._dictify(p.value), GraphSONWriter._dictify(p.other)]
        return _SymbolHelper.objectify("P", dict)


class BindingSerializer(GraphSONSerializer):
    def _dictify(self, binding):
        dict = {}
        dict["key"] = binding.key
        dict["value"] = GraphSONWriter._dictify(binding.value)
        return _SymbolHelper.objectify("Binding", dict)


class LambdaSerializer(GraphSONSerializer):
    def _dictify(self, lambdaObject):
        lambdaResult = lambdaObject()
        dict = {}
        script = lambdaResult if isinstance(lambdaResult, str) else lambdaResult[0]
        language = statics.default_lambda_language if isinstance(lambdaResult, str) else lambdaResult[1]
        dict["script"] = script
        dict["language"] = language
        if language == "gremlin-jython" or language == "gremlin-python":
            if not script.strip().startswith("lambda"):
                script = "lambda " + script
                dict["script"] = script
            dict["arguments"] = eval(dict["script"]).func_code.co_argcount
        else:
            dict["arguments"] = -1
        return _SymbolHelper.objectify("Lambda", dict)


class NumberSerializer(GraphSONSerializer):
    def _dictify(self, number):
        if isinstance(number, bool):  # python thinks that 0/1 integers are booleans
            return number
        elif isinstance(number, long):
            return _SymbolHelper.objectify("Int64", number)
        elif isinstance(number, int):
            return _SymbolHelper.objectify("Int32", number)
        elif isinstance(number, float):
            return _SymbolHelper.objectify("Float", number)
        elif isinstance(number, Decimal):
            return _SymbolHelper.objectify("BigDecimal", unicode(number), prefix='gx')
        else:
            return number


class StringSerializer(GraphSONSerializer):
    def _dictify(self, s):
        return six.text_type(s)


# DSE Types
class BlobSerializer(GraphSONSerializer):
    def _dictify(self, b):
        return base64.b64encode(b)


class InstantSerializer(GraphSONSerializer):
    def _dictify(self, d):
        return _SymbolHelper.objectify("Instant", d.isoformat(), prefix='gx')


class DurationSerializer(GraphSONSerializer):
    def _dictify(self, d):
        return _SymbolHelper.objectify("Duration", duration_isoformat(d), prefix='gx')


class PointSerializer(StringSerializer):
    def _dictify(self, p):
        return _SymbolHelper.objectify("Point", six.text_type(p), prefix='dse')


class LineStringSerializer(StringSerializer):
    def _dictify(self, l):
        return _SymbolHelper.objectify("LineString", six.text_type(l), prefix='dse')


class PolygonSerializer(StringSerializer):
    def _dictify(self, p):
        return _SymbolHelper.objectify("Polygon", six.text_type(p), prefix='dse')


'''
DESERIALIZERS
'''


class GraphSONDeserializer(object):
    @abstractmethod
    def _objectify(self, dict):
        return dict


class TraverserDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        return Traverser(GraphSONReader._objectify(dict[_SymbolHelper._VALUE]["value"]),
                         GraphSONReader._objectify(dict[_SymbolHelper._VALUE]["bulk"]))


class NumberDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        type = dict[_SymbolHelper._TYPE]
        value = dict[_SymbolHelper._VALUE]
        if type == "g:Int32" or type == "gx:Int16":
            return int(value)
        elif type == "g:Int64" or type == "gx:BigInteger":
            return long(value)
        elif type == "gx:BigDecimal":
            return Decimal(value)
        else:
            return float(value)


class VertexDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return Vertex(GraphSONReader._objectify(value["id"]), value["label"] if "label" in value else "")


class EdgeDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return Edge(GraphSONReader._objectify(value["id"]),
                    Vertex(GraphSONReader._objectify(value["outV"]), ""),
                    value["label"] if "label" in value else "vertex",
                    Vertex(GraphSONReader._objectify(value["inV"]), ""))


class VertexPropertyDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return VertexProperty(GraphSONReader._objectify(value["id"]), value["label"],
                              GraphSONReader._objectify(value["value"]))


class PropertyDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return Property(value["key"], GraphSONReader._objectify(value["value"]))


# class PathDeserializer(GraphSONDeserializer):
#     def _objectify(self, dict):
#         import pdb;pdb.set_trace()
#         value = dict[_SymbolHelper._VALUE]
#         labels = []
#         objects = []
#         for label in value["labels"]:
#             labels.append(set(label))
#         for object in value["objects"]:
#             objects.append(GraphSONReader._objectify(object))
#         return Path(labels, objects)


class _SymbolHelper(object):
    symbolMap = {"global_": "global", "as_": "as", "in_": "in", "and_": "and",
                 "or_": "or", "is_": "is", "not_": "not", "from_": "from",
                 "set_": "set", "list_": "list", "all_": "all"}

    _TYPE = "@type"
    _VALUE = "@value"

    @staticmethod
    def toGremlin(symbol):
        return _SymbolHelper.symbolMap[symbol] if symbol in _SymbolHelper.symbolMap else symbol

    @staticmethod
    def objectify(type, value, prefix="g"):
        return {_SymbolHelper._TYPE: prefix + ":" + type, _SymbolHelper._VALUE: value}


class UUIDDeserializer(GraphSONSerializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return uuid.UUID(value)


class InstantDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')


class BlobDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        decoded_value = base64.b64decode(value)
        return bytearray(decoded_value)

class DurationDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return parse_duration(value)

class PointDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return Point.from_wkt(value)


class LineStringDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return LineString.from_wkt(value)


class PolygonDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return Polygon.from_wkt(value)


serializers = {
    Traversal: BytecodeSerializer(),
    Traverser: TraverserSerializer(),
    Bytecode: BytecodeSerializer(),
    Binding: BindingSerializer(),
    P: PSerializer(),
    Enum: EnumSerializer(),
    FunctionType: LambdaSerializer(),
    LongType: NumberSerializer(),
    IntType: NumberSerializer(),
    FloatType: NumberSerializer(),

    uuid.UUID: StringSerializer(),
    Decimal: NumberSerializer(),
    #BigInteger: NumberSerializer(),
    #Int16: NumberSerializer(),
    datetime.datetime: InstantSerializer(),
    bytearray: BlobSerializer(),
    datetime.timedelta: DurationSerializer(),

    Point: PointSerializer(),
    LineString: LineStringSerializer(),
    Polygon: PolygonSerializer(),
}


if six.PY2:
    serializers.update({
        buffer: BlobSerializer(),
    })
else:
    serializers.update({
        memoryview: BlobSerializer(),
        bytes: BlobSerializer(),
    })

deserializers = {
    "g:Traverser": TraverserDeserializer(),
    "g:Int32": NumberDeserializer(),
    "g:Int64": NumberDeserializer(),
    "g:Float": NumberDeserializer(),
    "g:Double": NumberDeserializer(),
    "g:Vertex": VertexDeserializer(),
    "g:Edge": EdgeDeserializer(),
    "g:VertexProperty": VertexPropertyDeserializer(),
    "g:Property": PropertyDeserializer(),
    #"g:Path": PathDeserializer(),

    "g:UUID": UUIDDeserializer(),
    "gx:BigDecimal": NumberDeserializer(),
    "gx:BigInteger":  NumberDeserializer(),
    "gx:Int16": NumberDeserializer(),
    "gx:Instant": InstantDeserializer(),
    "gx:Duration": DurationDeserializer(),

    "dse:Blob": BlobDeserializer(),
    "dse:Point": PointDeserializer(),
    "dse:LineString": LineStringDeserializer(),
    "dse:Polygon": PolygonDeserializer()
}
