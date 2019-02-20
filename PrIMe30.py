import hashlib
from urllib.request import Request, urlopen
import urllib.parse
import json
import warnings

from generateMessage import * # Builds Elasticsearch messages


class Warehouse:
    def __init__(obj, user, password):
        #obj.Url = 'http://52.88.176.152:8080/'
        obj.Url = 'http://54.214.127.78:8080/'
        obj.user = user
        password = hashlib.md5(password.encode('utf-8')).hexdigest()

        urlreq = {'grant_type': 'password',
                  'username': obj.user,
                  'password': password}

        data = urllib.parse.urlencode(urlreq).encode('utf-8')

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        request = Request(obj.Url + '/token', data=data, headers=headers)

        try:
            response_body = urlopen(request).read()
        except:
            print('Authentication failed.')
            raise

        obj.response_body = json.loads(bytes.decode(response_body))
        obj.token = 'Bearer ' + obj.response_body['access_token']
        print('User Authenticated')

    def search(obj, category, field, query1, query2=''):
        '''
        Generate ElasticSearch Queries for PrIMe 3.0
        SEARCH(OBJ, CATEGORY, FIELD, QUERY1, QUERY2)

        CATEGORY is a character string specifying the collection
        of the warehouse which will searched within. Default 'all'.
        Other valid collections are listed below:

        all
        bibliography
        dataAttribute
        dataset
        element
        experiment
        instrumentalModel
        model
        optimizationVariable
        optimizationVariableBounds
        reaction
        reactionRate
        species
        surrogateModel
        thermodynamicData
        transportData

        FIELD is a string specifying the field of the
        category which will be queried. Each category has its own
        unique fields but some fields, i.e. primeID , are shared
        amongst all categories.

        QUERY is a string containing a search query string.

        '''

        validCategories = ['all', 'bibliography', 'dataAttribute',
                           'dataset', 'element', 'experiment', 'instrumentalModel',
                           'model', 'optimizationVariable', 'optimizationVariableBounds',
                           'reaction', 'reactionRate', 'species', 'surrogateModel',
                           'thermodynamicData', 'transportData']

        s = [x.lower() for x in validCategories]
        if not any(x in category.lower() for x in s):
            warnings.warn(
                'Input category is not valid. Using the default catagory "all".')
            category = 'all'

        if (field.lower() != 'rates') and (category.lower() != 'optimizationVariable'.lower()):
            msg = generateMessage(category, field, query1)
        else:
            # optimizationVariable & rates take two inputs reactionID and rateID (query1,query2) as inputs.
            msg = generateMessage(category, field, query1, query2)

        searchUrl = obj.Url + 'api/v2/warehouse/search/raw/' + category
        headers = {'Content-Type': 'text/plain', 'Authorization': obj.token}
        jsonString = json.dumps(json.loads(msg))
        data = jsonString.encode('utf-8')

        request = Request(searchUrl, data=data, headers=headers)
        response = urlopen(request).read()
        return(json.loads(bytes.decode(response)))

    def getXml(obj, pathTo):
        # Accepts PATHTO as a string of the location of XML document in the PrIMe Warehouse
        #
        if isinstance(pathTo, str):
            # Download XML from specified location
            searchUrl = obj.Url + 'api/v2/warehouse/content?path=' + pathTo
            headers = {'Authorization': obj.token}
            request = Request(searchUrl, headers=headers)
            response = urlopen(request).read()
            return(bytes.decode(response))
        else:
            raise TypeError

    def getFile(obj, pathTo):
        # Saves file from Warehouse to current working directory(cwd)
        # PATHTO is a string of the location of file on the PrIMe Warehouse
        #
        # Example: wh.getFile('depository/species/catalog/s00009193.xml')
        #
        #
        import shutil

        if isinstance(pathTo, str):
            # Count number of entries in collection
            fileName = pathTo[-13:]
            searchUrl = obj.Url + 'api/v2/warehouse/content?path=' + pathTo
            headers = {'Authorization': obj.token}
            request = Request(searchUrl, headers=headers)
            response = urlopen(request).read()
            f = open(fileName, 'wb')
            f.write(response)
            print(fileName + ' was saved in the current working directory.')
        else:
            raise TypeError

    def getCount(obj, collection):
        # Returns the number of entries of a specified collection
        #
        # COLLECTION is a string of the location which will be counted.
        # Examples:
        #
        #     depository/species/catalog
        #     depository/bibliography/catalog
        #     depository/experiments/catalog
        #

        if isinstance(collection, str):
            # Count number of entries in collection
            searchUrl = obj.Url + 'api/v2/warehouse/search/all/count?path=' + collection
            headers = {'Authorization': obj.token}
            request = Request(searchUrl, headers=headers)
            response = urlopen(request).read()
            return(int(bytes.decode(response)))
        else:
            raise TypeError

    def exist(obj, pathTo):
        # Returns logical True or False depending if pathTo exists
        # pathTo is a string of the location of file on the PrIMe Warehouse
        #
        # Example:
        #     wh.exist('depository/bibliography/catalog/b00000033.xml')   File exists
        #     wh.exist('depository/bibliography/catalog/f99000000.xml')   File does not exist
        #
        if isinstance(pathTo, str):
            searchUrl = obj.Url + 'api/v2/warehouse/xml/exist?path=' + pathTo
            headers = {'Authorization': obj.token}
            request = Request(searchUrl, headers=headers)
            try:
                response = urlopen(request).read()
                print('File exists.')
                return(True)
            except:
                print('File does not exist.')
                return(False)
        else:
            raise TypeError

    def getList(obj, collection):
        # Returns a list of all files by the specified path
        # COLLECTION is a string specifiying the collection of the warehouse
        #
        # Example:
        #     wh.getList('depository/bibliography/catalog')
        #     wh.getList('depository/reaction/catalog')
        #
        if isinstance(collection, str):
            searchUrl = obj.Url + 'api/v2/warehouse/search/all?path=' + collection
            headers = {'Authorization': obj.token}
            request = Request(searchUrl, headers=headers)
            response = urlopen(request).read()
            return(json.loads(bytes.decode(response)))
        else:
            raise TypeError

    def getProperty(obj, path, prop):
        # Returns a property value from a document.
        # PATH is the location of a document on the PrIMe Warehouse
        # PROP is the field name of the property whose value will be returned
        #
        # Example:
        #     wh.findProp('depository/bibliography/catalog/b00000290.xml', 'year')
        #     wh.findProp('depository/experiments/catalog/x00001327.xml', 'kind')
        #     wh.findProp('depository/reactions/data/r00013869/rk00000036.xml', 'value')
        #
        if all(isinstance(item, str) for item in [path, prop]):
            searchUrl = obj.Url + 'api/v2/warehouse/fields?path=' + path + '&field=' + prop
            headers = {'Authorization': obj.token}
            request = Request(searchUrl, headers=headers)
            response = urlopen(request).read()
            return(json.loads(bytes.decode(response)))
        else:
            raise TypeError

    def getPropertyNames(obj, collection):
        # Returns possible property names for a given collection
        # COLLECTION is a string specifying the collection of the PrIMe Warehouse
        #
        # Example:
        #     wh.getPropertyNames('experiment')
        #     wh.getPropertyNames('bibliography')
        #     wh.getPropertyNames('element')
        #
        if isinstance(collection, str):
            searchUrl = obj.Url + 'api/v2/warehouse/fields?path=' + collection
            headers = {'Authorization': obj.token}
            request = Request(searchUrl, headers=headers)
            response = urlopen(request).read()
            return(json.loads(bytes.decode(response)))
        else:
            raise TypeError

    def getJSON(obj, category, pathTo):
        # GETJSON will match the pathTo with the ID of a JSON document and return the document
        #
        if isinstance(pathTo, str):
            startString = '{ "ids": { "values": "'
            endString = '" } }'
            msg = startString + pathTo + endString

            searchUrl = obj.Url + 'api/v2/warehouse/search/raw/details/' + category
            headers = {'Content-Type': 'text/plain',
                       'Authorization': obj.token}
            jsonString = json.dumps(json.loads(msg))
            data = jsonString.encode('utf-8')
            request = Request(searchUrl, data=data, headers=headers)
            response = urlopen(request).read()
            return(json.loads(bytes.decode(response)))
        else:
            raise TypeError

    def getBoundsFromOptVar(obj, vbPath):
        # Takes a optimization variable bounds path and returns upper and lower bounds
        #
        if isinstance(vbPath, str):
            f = obj.getJSON('optimizationVariableBounds', vbPath)
            vbDoc = f[0]

            lower = float(vbDoc['optimizationVariableLink']
                          ['bounds']['lower']['#text'])
            upper = float(vbDoc['optimizationVariableLink']
                          ['bounds']['upper']['#text'])
            return((lower, upper))
        else:
            raise TypeError

    def getModelBounds(obj, pathToModel):
        # returns reactionNames and bounds from the PrIMe database
        #
        import numpy as np
        f = obj.getJSON('model', pathToModel)
        modelDoc = f[0]

        reactionKey = []
        rL = modelDoc['reactionSet']['reactionLink']
        reactionBounds = np.zeros((len(rL), 2))

        for i in range(0, len(rL)):
            reactionKey.append(rL[i]['reactionRateLink']['@preferredKey'])
            rID = rL[i]['@primeID']
            rkID = rL[i]['reactionRateLink']['@primeID']
            vResults = obj.search('optimizationVariable', 'rates', rID, rkID)

            if len(vResults) == 1:
                # single matching optimizationVariable Found
                variableID = vResults[0][-13:-4]
                vbResults = obj.search(
                    'optimizationVariableBounds', 'varlinkid', variableID)

                if len(vbResults) == 1:
                    # single results
                    reactionBounds[i, :] = obj.getBoundsFromOptVar(
                        vbResults[0])
                elif len(vbResults) == 2 and vbResults[0][-13:-4] == "vb00000000":
                    # two results but one is the vb0 file.
                    reactionBounds[i, :] = obj.getBoundsFromOptVar(
                        vbResults[1])
                else:
                    warnings.warn(
                        'Reaction :' + reactionKey[i] + ' has multiple bounds associated. Setting bounds to (0,0).')
                    reactionBounds[i, :] = (0, 0)

            elif len(vResults) == 0:
                # variable not found
                reactionBounds[i, :] = (0.5, 2)
            else:
                raise ValueError(
                    'Results contain multiple matches. Unable to proceed.')

        return(reactionKey, reactionBounds)

    def getTarget(obj, pathToTarget):
        # Returns a dictionary of property information from the specified target
        #

        def parseCommonProperties(commonPropNode):
            # internal function for parsing common properties of experimental document
            #
            propNames = []
            propValues = []
            propUnits = []

            speciesName = []
            speciesID = []
            speciesUnits = []
            molFrac = []

            if isinstance(commonPropNode['property'], list):
                for i in range(0, len(commonPropNode['property'])):
                    pName = commonPropNode['property'][i]['@name']
                    if pName == 'initial composition':
                        # fill-in initial composition
                        compNodes = commonPropNode['property'][i]['component']
                        for j in range(0, len(compNodes)):
                            speciesName.append(
                                compNodes[j]['speciesLink']['@preferredKey'])
                            speciesID.append(
                                compNodes[j]['speciesLink']['@primeID'])
                            try:
                                speciesUnits.append(
                                    compNodes[j]['amount']['@units'])
                            except:
                                speciesUnits.append([])

                            try:
                                molFrac.append(
                                    float(compNodes[j]['amount']['#text']))
                            except:
                                molFrac.append([])
                    else:
                        propNames.append(pName)
                        propValues.append(
                            float(commonPropNode['property'][i]['value']['#text']))
                        propUnits.append(
                            commonPropNode['property'][i]['@units'])
            elif isinstance(commonPropNode['property'], dict):
                if commonPropNode['property']['@name'] == 'initial composition':
                    compNodes = expDoc['commonProperties']['property']['component']
                    for i in range(0, len(compNodes)):
                        speciesUnits.append(compNodes[i]['amount']['@units'])
                        molFrac.append(float(compNodes[i]['amount']['#text']))
                        speciesName.append(
                            compNodes[i]['speciesLink']['@preferredKey'])
                        speciesID.append(
                            compNodes[i]['speciesLink']['@primeID'])

            return(propNames, propValues, propUnits, speciesName, speciesID, speciesUnits, molFrac)

        # Get Target function
        if not isinstance(pathToTarget, str):
            raise TypeError

        f = obj.getJSON('dataAttribute', pathToTarget)
        datastore = f[0]
        #
        # Take JSON and Organize Data into Python Dictionary (qoi)
        qoi = {}

        obsNodes = datastore['dataAttributeValue']['observable']
        if isinstance(obsNodes, list):
            targetType = datastore['dataAttributeValue']['observable'][0]['property']['@name']
        elif isinstance(obsNodes, dict):
            targetType = datastore['dataAttributeValue']['observable']['property']['@name']

        if targetType == 'laminar flame speed' or targetType == 'flame speed':
            qoiType = 'flame speed'
        elif targetType == 'time' or targetType == 'ignition delay':
            qoiType = 'time'

        # Indicator Node
        indicatorNode = datastore['dataAttributeValue']['indicator']
        propNames = []
        propValues = []
        propUnits = []

        if isinstance(indicatorNode, list):
            for i in range(0, len(indicatorNode)):
                propNames.append(indicatorNode[i]['property']['@name'])
                propValues.append(
                    float(indicatorNode[i]['property']['value']['#text']))
                propUnits.append(indicatorNode[i]['property']['@units'])

        elif isinstance(indicatorNode, dict):
            propNames.append(indicatorNode['property']['@name'])
            propValues.append(
                float(indicatorNode['property']['value']['#text']))
            propUnits.append(indicatorNode['property']['@units'])

        # Observable Node Uncertainty
        obsNode = datastore['dataAttributeValue']['observable']
        if isinstance(obsNode, list):
            obsNode = datastore['dataAttributeValue']['observable'][0]
        elif isinstance(obsNode, dict):
            obsNode = datastore['dataAttributeValue']['observable']

        lowerBound = float(obsNode['bounds']['lower']['#text'])
        upperBound = float(obsNode['bounds']['upper']['#text'])
        uncKind = obsNode['bounds']['@kind']
        uncBounds = [lowerBound, upperBound]

        # Node information
        obsValue = float(obsNode['property']['value']['#text'])
        obsUnits = obsNode['property']['@units']
        if qoiType == 'time':
            obsInd = obsNode['@derivedBy']
            obsIndSpecies = obsNode['@speciesName']
            obsIndSpeciesID = obsNode['@speciesID']

        # propertyLink
        dataGroupID = datastore['propertyLink'][0]['@dataGroupID']

        # Load expDoc
        expID = datastore['propertyLink'][0]['@experimentPrimeID']
        pathToExp = 'depository/experiments/catalog/' + expID + '.xml'
        expF = obj.getJSON('experiment', pathToExp)
        expDoc = expF[0]

        # Experiment Information
        expKind = expDoc['apparatus']['kind']['#text']
        expID = expDoc['@primeID']

        # Parse Common Properties
        commonPropNode = expDoc['commonProperties']
        [pNames, pValues, pUnits, speciesName, speciesID, speciesUnits,
            molFrac] = parseCommonProperties(commonPropNode)

        # Copy Common Properties Over Indicator Properties if name matches
        for i in range(0, len(pNames)):
            for j in range(0, len(propNames)):
                if pNames[i] == propNames[j]:
                    propValues[j] = pValues[i]

        # Check if molFrac from Parse Common Properties is empty
        if not molFrac[0]:
            # if empty go through datagroup of expDoc
            # match obsValue with associated property name and
            # get all property and information from that dataGroup/dataPoint
            dpValues, dpUnits, dpNames = ([] for n in range(3))
            sNames, sUnits, sID, mF = ([] for n in range(4))

            # if multiple datagroups exist
            if isinstance(expDoc['dataGroup'], list):
                for k in range(0, len(expDoc['dataGroup'])):
                    if expDoc['dataGroup'][k]['@id'] == dataGroupID:
                        eDG = expDoc['dataGroup'][k]
            elif isinstance(expDoc['dataGroup'], dict):
                eDG = expDoc['dataGroup']
            else:
                raise TypeError

            for i in range(0, len(eDG['property'])):
                if eDG['property'][i]['@name'] == qoiType:
                    matchingPropID = eDG['property'][i]['@id']

                    for j in range(0, len(eDG['dataPoint'])):
                        if float(eDG['dataPoint'][j][matchingPropID]['#text']) == obsValue:
                            matchingDP = j

            for i in range(0, len(eDG['property'])):
                # get all information from dataPoint node
                cList = ['composition', 'concentration']
                s = [x.lower() for x in cList]
                if any(x in eDG['property'][i]['@name'] for x in s):

                    if len(speciesID) != 0:
                        # match speciesID with sID
                        for j in range(0, len(speciesID)):
                            speLID = eDG['property'][i]['speciesLink']['@primeID']
                            if speLID == speciesID[j]:
                                matchingID = eDG['property'][i]['@id']
                                speciesUnits[j] = eDG['property'][i]['@units']
                                molFrac[j] = float(
                                    eDG['dataPoint'][matchingDP][matchingID]['#text'])

                    else:
                        # no common property of composition
                        matchingID = eDG['property'][i]['@id']
                        sID.append(eDG['property'][i]
                                   ['speciesLink']['@primeID'])
                        sUnits.append(eDG['property'][i]['@units'])
                        sNames.append(eDG['property'][i]['@name'])
                        mF.append(
                            float(eDG['dataPoint'][matchingDP][matchingID]['#text']))

                else:
                    dpNames.append(eDG['property'][i]['@name'])
                    dpUnits.append(eDG['property'][i]['@units'])
                    pID = eDG['property'][i]['@id']
                    try:
                        dpValues.append(
                            float(eDG['dataPoint'][matchingDP][pID]['#text']))
                    except:
                        # if individual uncetainty is applied to a node there is a comma delimination
                        # Example: 0.07,0.002
                        #
                        s = eDG['dataPoint'][matchingDP][pID]['#text'].split(
                            ',')
                        dpValues.append([float(s[0]), float(s[1])])

            # extend list with common property values
            propNames.extend(pNames)
            propValues.extend(pValues)
            propUnits.extend(pUnits)

            # If dp property name matches one of the common properties names.
            # Keep Common Property,  Delete DataPoint information.
            #
            # Common example of this is the ambient temperature of the enviroment in a flame experiment.
            # Common Properties has the temperature of enviroment while dataPoint may contain flame temperature
            # which is unused by simulation
            #
            # Remove if statement if you want all properties returned, including duplicate property names
            #
            if len(dpNames) != 0:
                for i in range(0, len(dpNames)):
                    for j in range(0, len(propNames)):
                        if dpNames[i] == propNames[j]:
                            dpNames.remove(dpNames[i])
                            dpUnits.remove(dpUnits[i])
                            dpValues.remove(dpValues[i])

            try:
                propNames.extend(dpNames)
                propValues.extend(dpValues)
                propUnits.extend(dpUnits)
            except:
                pass

            # add species information to speciesLists
            if len(sNames) != 0:
                speciesName.append(sNames)
                speciesID.append(sID)
                speciesUnits.append(sUnits)
                molFrac.append(mF)

        # Create new dictionary of QOI information
        qoi['indicator_name'] = propNames
        qoi['indicator_value'] = propValues
        qoi['indicator_units'] = propUnits
        qoi['preferredKey'] = datastore['preferredKey']['#text']
        qoi['observable_bounds'] = uncBounds
        qoi['observable_boundkind'] = uncKind
        qoi['observable_value'] = obsValue
        qoi['observable_units'] = obsUnits
        if qoiType == 'time':
            qoi['derivedBy'] = obsInd
            qoi['derivedBy_speciesName'] = obsIndSpecies
            qoi['derivedBy_speciesID'] = obsIndSpeciesID

        qoi['experiment_ID'] = expID
        qoi['experiment_type'] = expKind
        qoi['species_key'] = speciesName
        qoi['species_molFraction'] = molFrac
        qoi['species_primeID'] = speciesID
        qoi['species_units'] = speciesUnits

        return(qoi)
