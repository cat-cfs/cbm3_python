# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

class DisturbanceExtension(object):
    '''
    Author: Max Fellows

    2014 Jun 13, modified by Scott for NETNET2014 python scripting

    Represents a disturbance event to be extended into the future.

    :param title: the name of the disturbance event to extend
    :type title: str
    :param defaultDistTypeIDs: the default disturbance type IDs corresponding to
        the event to extend
    :type defaultDistTypeIDs: list of int
    :param fromYear: the last year the disturbance event occurs; used as a
        template for all future years
    :type fromYear: int
    :param toYear: the year to extend the disturbance event to (inclusive)
    :type toYear: int
    '''

    def __init__(self, title, defaultDistTypeIDs, fromYear, toYear):
        self.__title = title
        self.__defaultDistTypeIDs = defaultDistTypeIDs
        self.__fromYear = fromYear
        self.__toYear = toYear


    def __str__(self):
        return """
               Disturbance Event: %(title)s
               From Year: %(fromYear)i
               To Year: %(toYear)i
               Default Disturbance Type IDs: %(defaultDistTypeIDs)s
               """ % {"title"             : self.__title,
                      "fromYear"          : self.__fromYear,
                      "toYear"            : self.__toYear,
                      "defaultDistTypeIDs": ", ".join(str(id) for id
                                                      in self.__defaultDistTypeIDs)}


    def getTitle(self):
        '''
        :returns: the name of this disturbance event
        :rtype: str
        '''
        return self.__title


    def getDefaultDistTypeIDs(self):
        '''
        :returns: the default disturbance type IDs corresponding to this event
        :rtype: list of int
        '''
        return self.__defaultDistTypeIDs


    def getFromYear(self):
        '''
        :returns: the last year this event occurs in the project databases,
            which will be used as a template when extending the event
        :rtype: int
        '''
        return self.__fromYear


    def getToYear(self):
        '''
        :returns: the year to extend this disturbance event to (inclusive)
        :rtype: int
        '''
        return self.__toYear
