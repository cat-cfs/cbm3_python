import pyodbc
import pandas as pd


def __get_connection_string(path):
    return \
        "Driver={Microsoft Access Driver (*.mdb, *.accdb)};" \
        f"User Id='admin';Dbq={path}"


def __as_data_frame(query, access_db_path, params=None):
    with pyodbc.connect(__get_connection_string(access_db_path)) as connection:
        df = pd.read_sql(query, connection, params)
    return df


def __execute_query(access_db_path, query, param_list):
    """Execute the specified query and params on the specified access_db_path
    """
    connection_string = __get_connection_string(access_db_path)
    with pyodbc.connect(connection_string) as connection:
        cursor = connection.cursor()
        for params in param_list:
            cursor.execute(query, params)


def __get_eligibility_criteria(db_path, eligibility_criteria_id):
    query = f"""
    SELECT
    tblDisturbanceEventCriteriaLookup.CriteriaID,
    tblDisturbanceEventCriteriaLookup.CriteriaTypeID,
    tblDisturbanceEventCriteriaType.Name,
    tblDisturbanceEventCriteriaLookup.CRValue
    FROM tblDisturbanceEventCriteriaType
    INNER JOIN tblDisturbanceEventCriteriaLookup ON (
        tblDisturbanceEventCriteriaLookup.CriteriaTypeID =
            tblDisturbanceEventCriteriaType.CriteriaTypeID
    ) AND (
        tblDisturbanceEventCriteriaType.CriteriaTypeID =
            tblDisturbanceEventCriteriaLookup.CriteriaTypeID
    ) WHERE tblDisturbanceEventCriteriaLookup.CriteriaID =
        {eligibility_criteria_id};
    """
    return __as_data_frame(query, db_path)


def __get_eligibility_criteria_name_lookup(db_path):
    query = "select CriteriaTypeID, Name from tblDisturbanceEventCriteriaType"
    return {
        row.Name: int(row.CriteriaTypeID)
        for i_row, row in __as_data_frame(query, db_path).iterrows()}


def __get_next_criteria_id(db_path, eligibility_criteria_id):
    df = __as_data_frame(
        "SELECT CriteriaID from tblDisturbanceEventCriteria", db_path)
    i = eligibility_criteria_id + 1
    id_set = set(df.CriteriaID)
    while True:
        if i not in id_set:
            return i
        else:
            i += 1


def __update_eligibility_template(new_criteria_id, criteria_template, update,
                                  criteria_lookup):
    update_criteria_type_id = criteria_lookup[update["Name"]]
    # if the specified criteria type id already exists in the template rows
    # update the value for that row
    if (criteria_template.CriteriaTypeID == update_criteria_type_id).any():
        criteria_template.loc[
            criteria_template.CriteriaTypeID == update_criteria_type_id,
            "CRValue"
        ] = update["Value"]
    # otherwise insert a new row with the update value
    else:
        new_row = pd.DataFrame(
            columns=["CriteriaID", "CriteriaTypeID", "Name", "CRValue"],
            data=[
                [new_criteria_id,
                 update_criteria_type_id,
                 update["Name"],
                 update["Value"]]])
        criteria_template = criteria_template.append(new_row)
    criteria_template.CriteriaID = new_criteria_id
    return criteria_template


def __get_updates(original_criteria_id, new_criteria_id, eligibility_update,
                  criteria_lookup):
    return [
        {
            "query": """
                INSERT INTO tblDisturbanceEventCriteria
                (CriteriaiD, Description) VALUES (?, ?)""",
            "params": [
                (new_criteria_id, eligibility_update["new_criteria_label"])]
        },
        {
            "query": """
                INSERT INTO tblDisturbanceEventCriteriaLookup
                (CriteriaID, CriteriaTypeID, CRValue) VALUES (?, ?, ?)""",
            "params": [
                (int(a), int(b), int(c)) for a, b, c in
                criteria_lookup[
                    ["CriteriaID", "CriteriaTypeID", "CRValue"]].itertuples(
                        index=False, name=None)]

        },
        {
            "query": """
                UPDATE tblDisturbanceEvents SET
                    tblDisturbanceEvents.CriteriaID = ?
                    WHERE tblDisturbanceEvents.CriteriaID = ? AND
                    tblDisturbanceEvents.TimeStepStart >= ?""",
            "params": [
                (new_criteria_id, original_criteria_id,
                 eligibility_update["start_timestep"])]
        }]


def get_nir_harvest_criteria_id(db_path):
    """Gets the single eligibility criteria id associated with the NIR
    harvest default disturbance type ids and the first timestep

    Args:
        db_path (str): path to an NIR project

    Returns:

        int: the NIR harvest eligibility criteria ID
    """

    query = """
        SELECT
            tblDisturbanceEvents.CriteriaID
        FROM tblDisturbanceType INNER JOIN (
            tblDisturbanceGroupScenario INNER JOIN tblDisturbanceEvents
                ON tblDisturbanceGroupScenario.DisturbanceGroupScenarioID =
                    tblDisturbanceEvents.DisturbanceGroupScenarioID
            ) ON tblDisturbanceType.DistTypeID =
                tblDisturbanceGroupScenario.DistTypeID
        WHERE tblDisturbanceType.DefaultDistTypeID In (4,195,196) AND
              tblDisturbanceEvents.TimeStepStart = 1
        GROUP BY
            tblDisturbanceType.DefaultDistTypeID,
            tblDisturbanceEvents.CriteriaID"""

    df = __as_data_frame(query, db_path)
    if df.shape[0] != 1:
        raise ValueError(
            f"failed to find single harvest criteria id for project {db_path}")
    else:
        return int(df.CriteriaID.iloc[0])


def get_criteria_updates(project_path, eligibility_update,
                         existing_criteria_id):
    """
    Gets a list of SQL updates and parameter tuples for updating CBM3
    disturbance event criteria and the related events themselves based
    on an existing template criteria id.

    Example::

        get_criteria_updates(
            project_path="AB.mdb",
            existing_criteria_id=500,
            eligibility_update = {
                "new_criteria_label": "relaxed harvest criteria",
                "start_timestep": 10,
                "updates": [
                    {"Name": "Minimum Softwood Age", "Value": 40},
                    {"Name": "Minimum Hardwood Age", "Value": 40}]})

        In this example a new eligibility criteria based on an existing
        eligibility criteria is create and the values in the updates list
        are either updated, if they already exist, or appended into
        tblEligibilityCriteriaLookup.

        Using the values in the example, the script will then update values in
        tblDisturbanceEvents with eligibility criteria id == 500
        and timestep >= 10. Those rows' criteria ID will be updated with the
        new internally generated criteria id.

    Args:
        project_path (str): path of the project to for which to get updates
        eligibility_update (dict): a dictionary describing the update
            configuration (see example)
        existing_criteria_id (int): the integer CriteriaID as defined in the
            specified CBM3 project.

    Returns:
        list: list of dictionaries with keys that will update the eligibility
            criteria:

             - query: the query to execute that will update the database
             - params: the parameters for the query
    """
    # set up: create a lookup of eligibility criteria names to CriteriaTypeID
    criteria_lookup = __get_eligibility_criteria_name_lookup(project_path)

    # select the existing harvest criteria rows to use as a
    # template for the new criteria.
    criteria_template = __get_eligibility_criteria(
        project_path, existing_criteria_id)
    if criteria_template.shape[0] == 0:
        raise ValueError(
            f"specified criteria id {existing_criteria_id} did not match any "
            "existing eligibilty criteria")

    # apply updates to the template
    new_criteria_id = __get_next_criteria_id(
        project_path, existing_criteria_id)
    for update in eligibility_update["updates"]:
        criteria_template = __update_eligibility_template(
            new_criteria_id, criteria_template, update, criteria_lookup)

    # get the updated values and sql for tblDisturbanceEventCriteria,
    # tblDisturbanceEventCriteriaLookup and tblDisturbanceEvents
    updates = __get_updates(
        existing_criteria_id, new_criteria_id, eligibility_update,
        criteria_template)
    return updates


def update_database(project_path, updates):
    """
    Update the dataase at the specified project path with the specified
    updates.  The updates are in the format returned by the
    `get_criteria_updates` function.

    Args:
        project_path (str): path of the project to update
        updates (list): list of dictionaries with keys that will update the
            eligibility criteria
    """
    for update in updates:
        __execute_query(project_path, update["query"], update["params"])
