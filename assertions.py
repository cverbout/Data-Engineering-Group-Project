def validate_data(record):

    errors=[]

    #Assertion: Each reading has an EVENT_NO_TRIP associated with it.
    if 'EVENT_NO_TRIP' not in record:
        errors.append("Missing EVENT_NO_TRIP")

    #Assertion: If there is a GPS_LONGITUDE, there must also be a GPS_LATITUDE for the reading
    if 'GPS_LONGITUDE' in record and 'GPS_LATITUDE' not in record:
        errors.append("Missing GPS_LATITUDE for GPS_LONGITUDE")

    #Assertion: Each reading must also have a GPS_SATELLITES value associated with it
    if 'GPS_SATELLITES' not in record:
        errors.append("Missing GPS_SATELLITES")

    #Assertion: GPS_SATELLITES values in the table must be greater than or equal to zero
    if 'GPS_SATELLITES' in record and record['GPS_SATELLITES'] is not None and record['GPS_SATELLITES'] < 0:
        errors.append("GPS_SATELLITES value must be greater than or equal to zero")

    #Assertion: Each reading must have a GPS_LATITUDE value
    if 'GPS_LATITUDE' not in record:
        errors.append("Missing GPS_LATITUDE")

    #Assertion: Each reading must have a GPS_LONGITUDE value
    if 'GPS_LONGITUDE' not in record:
        errors.append("Missing GPS_LONGITUDE")

    #Assertion: ACT_TIME values in the BreadCrumb table must be less than 86399 (maximum seconds in a day)
    if 'ACT_TIME' in record and record['ACT_TIME'] >= 86399:
        errors.append("ACT_TIME value must be less than 86399 (maximum seconds in a day)")

    #Assertion: GPS_LATITUDE values in the BreadCrumb table must be within the range of -90 to 90 degrees.
    if 'GPS_LATITUDE' in record and (record['GPS_LATITUDE'] is not None) and (record['GPS_LATITUDE'] < -90 or record['GPS_LATITUDE'] > 90):
        errors.append("GPS_LATITUDE value must be within the range of -90 to 90 degrees")

    #Assertion: GPS_LONGITUDE values in the BreadCrumb table must be within the range of -180 to 180 degrees.
    if 'GPS_LONGITUDE' in record and (record['GPS_LONGITUDE'] is not None) and (record['GPS_LONGITUDE'] < -180 or record['GPS_LONGITUDE'] > 180):
        errors.append("GPS_LONGITUDE value must be within the range of -180 to 180 degrees")

    #Assertion: ACT_TIME values in the BreadCrumb table must not be negative
    if 'ACT_TIME' in record and record['ACT_TIME'] < 0:
        errors.append("ACT_TIME value must not be negative")

    return errors

