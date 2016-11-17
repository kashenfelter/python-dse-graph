# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms


from gremlin_python.process.traversal import P


class GeoP(P):
    pass


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


class Geo(object):

    @staticmethod
    def inside(value):
        """
        Search any instance of geometry inside the Distance targeted.

        :param value: A Distance to look for.
        """
        return GeoP('inside', value)
