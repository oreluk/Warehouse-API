def generateMessage(category, field, term, term2=''):
    # Generate Elasticsearch Message
    #
    # GENERATEMESSAGE(CATEGORY, FIELD, TERM) will take three character
    # strings and return an elastic search query.
    # CATEGORY is a character string specifying the type of
    # catagory will searched. Default 'all'. Other available
    # options are listed below:
    #
    # all
    # bibliography
    # dataAttribute
    # dataset
    # element
    # experiment
    # instrumentalModel
    # model
    # optimizationVariable
    # reactions
    # reactionRate
    # species
    # surrogateModel
    # thermodynamicData
    # transportData
    #
    # FIELD is a character string specifying the field to be searched for. This
    # often is a node name or attribute name in the document.
    #
    # TERM is a character string of the term which a query message will be
    # generated for.
    #

    def generateDefaultMessage(field, term):
        #
        # INTERNAL FUNCTION which will generated a default message from
        # MESSAGE = GENERATEDEFAULTMESSAGE(FIELD, TERM)
        #

        msg = ('[{ "query_string": { "fields": ["*' +
               field + '*"], "query": "' +
               term + '" }}]')
        return(msg)

    # Conditional Statements
    if category.lower() == 'species':
        if field.lower == 'preferredkey':
            msg = '[{ "match": "preferredKey.#text": "' + term + '"} }]'

        elif field.lower() == 'formula':
            msg = ('[ { "nested": { "path": "chemicalIdentifier.name",' +
                   ' "query": { "bool": { "must": [ { "match": {' +
                   ' "chemicalIdentifier.name.@type": "formula" } },' +
                   ' { "match_phrase": { "chemicalIdentifier.name.#text": "' + term +
                   '" } } ] } } } } ]')

        elif field.lower() == 'brutoformula':
            msg = ('[ { "nested": { "path": "chemicalIdentifier.name",' +
                   ' "query": { "bool": { "must": [ { "match": {' +
                   ' "chemicalIdentifier.name.@type": "formula" } },' +
                   ' { "match_phrase": { "chemicalIdentifier.name.#text": "' + term +
                   '" } } ] } } } } ]')

        elif field.lower() == 'caseregistrynumber':
            msg = (' [ { "nested": { "path": "chemicalIdentifier.name",' +
                   ' "query": { "bool": { "must": [ { "match": {' +
                   ' "chemicalIdentifier.name.@type": "CASRegistryNumber"' +
                   ' } }, { "match_phrase": { "chemicalIdentifier.name.#text": "' + term +
                   '"} } ] } } } } ]')

        elif field.lower() == 'inchi':
            msg = ('[ { nested: { path: "chemicalIdentifier.name", query: ' +
                   ' { bool: { must: [ { match: { "chemicalIdentifier.name.@type":' +
                   ' "InChI" } }, { match_phrase: { "chemicalIdentifier.name.#text": "' + term +
                   '"} } ] } } } } ]')

        elif field.lower() == 'composition':
            # TODO LATER
            msg = generateDefaultMessage(field, term)

        elif field.lower() == 'name':
            msg = ('[ { "nested": { "path": "chemicalIdentifier.name", "query": ' +
                   ' { "bool": { "must": [  { "match_phrase": { "chemicalIdentifier.name.#text": "' + term +
                   '" } } ] } } } } ]')

        else:
            msg = generateDefaultMessage(field, term)

    elif category.lower() == 'experiment':
        if field.lower() == 'additionaldataitem':
            msg = ('[ { "nested": { "path": "additionalDataItem", "query": {' +
                   ' "bool": { "must": [ { "query_string": { "fields": ' +
                   ' ["*additionalDataItem*"], "query": "' + term + '"} }' +
                   ' ] } } } } ]')

        else:
            msg = generateDefaultMessage(field, term)

    elif category.lower() == 'optimizationvariable':
        if field.lower() == 'rates':
            msg = ('[ { "match_phrase": {' +
                   ' "variable.actualVariable.propertyLink.@reactionPrimeID":  "' + term +
                   '" } }, { "match_phrase": {' +
                   '"variable.actualVariable.propertyLink.@reactionRatePrimeID": "' + term2 +
                   '" } } ]')
        else:
            msg = generateDefaultMessage(field, term)

    elif category.lower() == 'optimizationvariablebounds':
        if field.lower() == 'varlinkid':
            msg = ('[ { "match": {' +
                   ' "optimizationVariableLink.@primeID":  "' + term +
                   '" } } ]')

        else:
            msg = generateDefaultMessage(field, term)

    else:
        msg = generateDefaultMessage(field, term)

    startString = '{ "bool": { "must": '
    endString = '"must_not": { "match": { "PathTo": "*/_attic/*" } } } }'
    msg = startString + msg + ',' + endString
    return(msg)
