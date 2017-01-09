# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from gremlin_python.process.traversal import P


class GeoP(object):

    def __init__(self, operator, value, other=None):
        self.operator = operator
        self.value = value
        self.other = other

    @staticmethod
    def inside(*args):
        return GeoP("inside", *args)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.operator == other.operator and self.value == other.value and self.other == other.other

    def __repr__(self):
        return self.operator + "(" + str(self.value) + ")" if self.other is None else self.operator + "(" + str(self.value) + "," + str(self.other) + ")"


class TextDistanceP(object):

    def __init__(self, operator, value, distance):
        self.operator = operator
        self.value = value
        self.distance = distance

    @staticmethod
    def fuzzy(*args):
        return TextDistanceP("fuzzy", *args)

    @staticmethod
    def token_fuzzy(*args):
        return TextDistanceP("tokenFuzzy", *args)

    @staticmethod
    def phrase(*args):
        return TextDistanceP("phrase", *args)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.operator == other.operator and self.value == other.value and self.distance == other.distance

    def __repr__(self):
        return self.operator + "(" + str(self.value) + "," + str(self.distance) + ")"


class Search(object):

    @staticmethod
    def token(value):
        """
        Search any instance of a certain token within the text property targeted.

        :param value: the value to look for.
        """
        return P('token', value)

    @staticmethod
    def token_prefix(value):
        """
        Search any instance of a certain token prefix withing the text property targeted.

        :param value: the value to look for.
        """
        return P('tokenPrefix', value)

    @staticmethod
    def token_regex(value):
        """
        Search any instance of the provided regular expression for the targeted property.

        :param value: the value to look for.
        """
        return P('tokenRegex', value)

    @staticmethod
    def prefix(value):
        """
        Search for a specific prefix at the beginning of the text property targeted.

        :param value: the value to look for.
        """
        return P('prefix', value)

    @staticmethod
    def regex(value):
        """
        Search for this regular expression inside the text property targeted.

        :param value: the value to look for.
        """
        return P('regex', value)

    @staticmethod
    def fuzzy(value, distance):
        """
        Search for a fuzzy string inside the text property targeted.

        :param value: the value to look for.
        :param distance: The distance for the fuzzy search. ie. 1, to allow a one-letter misspellings.
        """
        return TextDistanceP.fuzzy(value, distance)

    @staticmethod
    def token_fuzzy(value, distance):
        """
        Search for a token fuzzy inside the text property targeted.

        :param value: the value to look for.
        :param distance: The distance for the token fuzzy search. ie. 1, to allow a one-letter misspellings.
        """
        return TextDistanceP.token_fuzzy(value, distance)

    @staticmethod
    def phrase(value, proximity):
        """
        Search for a phrase inside the text property targeted.

        :param value: the value to look for.
        :param proximity: The proximity for the phrase search. ie. phrase('David Felcey', 2).. to find 'David Felcey' with up to two middle names.
        """
        return TextDistanceP.phrase(value, proximity)


class Geo(object):

    @staticmethod
    def inside(value):
        """
        Search any instance of geometry inside the Distance targeted.

        :param value: A Distance to look for.
        """
        return GeoP('inside', value)
