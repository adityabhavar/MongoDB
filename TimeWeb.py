import ast
import datetime
import json
import time

import numpy as np
import pandas as pd
import requests

from . import sql_server as sq

# import sql_server as sq


def get_access_token():
    """
    Extracts access_token

    Args
    ----
    Returns:
    access_token(str) : get_access_token

    Example:
    access_token = get_access_token()

    """
    url = ''

    payload = {
       
    }

    response = requests.post(url=url, json=payload)
    response_json = response.json()

    return response_json


def get_date():
    """
    Get date 

    Args
    ----
    Returns:
    effectivedate(str) : get_date

    Example:
    effectivedate = get_date()

    """
    start = str(datetime.datetime.today().date() -
                datetime.timedelta(days=1)) + ' '+"00:00:00"
    start_date = int(time.mktime(datetime.datetime.strptime(
        start, "%Y-%m-%d %H:%M:%S").timetuple()))*1000
    print(start_date)

    end = str(datetime.datetime.today().date()) + ' ' + "00:00:00"
    end_date = int(time.mktime(datetime.datetime.strptime(
        end, "%Y-%m-%d %H:%M:%S").timetuple()))*1000
    print(end_date)

    return start_date, end_date


def response_code_not_200(response):
    response_status_code = response.status_code
    response_text = response.text
    response_headers = response.headers
    response_url = response.url
    raise Exception(f"""
    Following is response status code
    "{response_status_code}"

   Following is response url
    "{response_url}"

   Following is response text
    "{response_text}"
    
    Following is response headers
    "{response_headers}"
    """)


def df_list_and_dict_col(explode_df: pd.DataFrame, col_name: str, drop_col=True) -> pd.DataFrame:

    explode_df[col_name] = explode_df[col_name].replace('', np.nan)
    explode_df[col_name] = explode_df[col_name].astype(
        str)  # to make sure that entire column is string
    explode_df[col_name] = explode_df[col_name].apply(ast.literal_eval)
    explode_df = explode_df.explode(col_name)
    explode_df = explode_df.reset_index(drop=True)
    normalized_df = pd.json_normalize(explode_df[col_name])

    explode_df = explode_df.join(
        other=normalized_df,
        lsuffix="_left",
        rsuffix="_right"
    )
    if drop_col:
        explode_df = explode_df.drop(columns=[col_name])

    return explode_df


def GetUserBasic(access_token: str, effectivedate: int):
    """
    Get user basic details

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user basic details
    """

    url = ""

    payload = json.dumps({
        "AuthToken": access_token,
        "EffectiveDate": f"""/Date({effectivedate}+0000)/""",
        "DataAction": {
            "Name": "SELECT-ALL",
            "Values": [
                ""
            ]
        }
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        response_code_not_200(response)
    data = response.json()
    df = pd.json_normalize(data['Results'])

    return df


def TimeGetPunchesByEmpIdentifier(access_token: str, effectivedate: int, effectivedate1: int) -> pd.DataFrame:
    """
    Get user time of punches

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    effectivedate1 : int
        Ending date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user time of punches
    """

    get_punch = []
    df = GetUserBasic(access_token, effectivedate)
    list_identifier = [str(item) for item in df['EmpIdentifier'].tolist()]
    batch_size = 50
    batch_identifier = [list_identifier[i:i + batch_size]
                        for i in range(0, len(list_identifier), batch_size)]
    for i in batch_identifier:
        print(i)
        url = ""

        payload = json.dumps({
            "AuthToken": access_token,
            "EmpIdentifierList": i,
            "StartDate": f"""/Date({effectivedate}+0000)/""",
            "EndDate": f"""/Date({effectivedate1}+0000)/""",
            "IgnoreLaborLevelCodes": True,
            "SearchAction": 0,
            "IncludeKioskTerminalInfo": True
        })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            response_code_not_200(response)
        data = response.json()
        df = pd.json_normalize(data)
        if 'OTInfo' in df:
            df = df_list_and_dict_col(df, 'OTInfo', drop_col=True)
            get_punch.append(df)
        elif df.any:
            get_punch.append(df)
        else:
            print('DataFrame is empty')

    get_punches = pd.concat(get_punch, ignore_index=True)

    return get_punches


def TimeExport(access_token: str, effectivedate: int, effectivedate1: int) -> pd.DataFrame:
    """
    Get employee time of export

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    effectivedate1 : int
        Ending date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of employee export time
    """
    get_time = []
    df = GetUserBasic(access_token, effectivedate)
    list_identifier = [str(item) for item in df['EmpIdentifier'].tolist()]
    batch_size = 50
    batch_identifier = [list_identifier[i:i + batch_size]
                        for i in range(0, len(list_identifier), batch_size)]
    for i in batch_identifier:
        print(i)
        url = ""

        payload = json.dumps({
            "AuthToken": access_token,
            "EmpIdentifiers": i,
            "StartDate": f"""/Date({effectivedate}+0000)/""",
            "EndDate": f"""/Date({effectivedate1}+0000)/""",
            "ExportMatrixSelectionType": 0,
            "EnableDuplicateDifferentialAsRegular": True,
            "EnableDuplicateOvertimeAsRegular": True,
            "FilterByTimeCardApprovalLevel": True
        })

        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            response_code_not_200(response)
        data = response.json()
        df = pd.json_normalize(data)
        get_time.append(df)

    TimeExport = pd.concat(get_time, ignore_index=True)

    return TimeExport


def GetUserTimeOnStatusBoard(access_token: str, effectivedate: int) -> pd.DataFrame:
    """
    Get user time status board

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user time status board
    """

    user_data = []
    url = ""
    df = GetUserBasic(access_token, effectivedate)
    list_identifier = [str(item) for item in df['EmpIdentifier'].tolist()]
    batch_size = 50
    batch_identifier = [list_identifier[i:i + batch_size]
                        for i in range(0, len(list_identifier), batch_size)]
    for i in batch_identifier:
        print(i)
        payload = json.dumps(
            {
                "AuthToken": access_token,
                "DataAction": {
                    "Name": "SELECT-EMPID",
                    "Values": i
                }
            }
        )

        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            response_code_not_200(response)
        data = response.json()
        df = pd.json_normalize(data['Results'])
        user_data.append(df)

    met_df = pd.concat(user_data, ignore_index=True)

    return met_df


# The resource you are looking for has been removed, had its name changed, or is temporarily unavailable.
def GetPunchPrompts(access_token: str, effectivedate: int) -> pd.DataFrame:
    """
    Get user punch prompts

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user punch prompts
    """
    url = ""
    get_punch = []
    df = GetUserBasic(access_token, effectivedate)
    list_identifier = [str(item) for item in df['EmpIdentifier'].tolist()]
    batch_size = 50
    batch_identifier = [list_identifier[i:i + batch_size]
                        for i in range(0, len(list_identifier), batch_size)]
    for i in batch_identifier:
        print(i)
        payload = json.dumps(
            {
                "AuthToken": access_token,
                "DataAction": {
                    "Name": "SELECT-EMPID",
                    "Values": i
                }
            }
        )

        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            response_code_not_200(response)
        data = response.json()
        df = pd.json_normalize(data['Results'])
        get_punch.append(df)

    punch_data = pd.concat(get_punch, ignore_index=True)

    return punch_data


def GetTimeOffPolicyDetailRule(access_token: str) -> pd.DataFrame:
    """
    Get user time of policy details

    Args
    ----
    access_token : str
        Access token to get the requests response 

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user time of policy details
    """
    url = ""
    payload = json.dumps(
        {
            "AuthToken": access_token,
            "DataAction": {
                "Name": "SELECT-TOPD-ID",
                "Values": ["1", "2", "3"]
            }
        }
    )

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        response_code_not_200(response)
    data = response.json()
    df = pd.json_normalize(data['Results'])
    return df


def GetTimePunchState(access_token: str, effectivedate: int) -> pd.DataFrame:
    """
    Get user time punch state

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    effectivedate1 : int
        Ending date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user punch state
    """
    url = ""
    get_time_punch = []
    df = GetUserBasic(access_token, effectivedate)
    for i in df['EmpIdentifier'].tolist():
        print(i)
        payload = json.dumps(
            {
                "AuthToken": access_token,
                "DataAction": {
                    "Name": "SELECT-EMPID",
                    "Values": [f"""{i}"""]
                }
            }
        )

        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)
        data = response.json()
        df = pd.json_normalize(data['Results'])
        get_time_punch.append(df)

    time_punch = pd.concat(get_time_punch, ignore_index=True)

    return time_punch


def GetUserDetail(access_token: str, effectivedate: int) -> pd.DataFrame:
    """
    Get user basic details

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user basic details
    """
    url = ""

    payload = json.dumps(
        {
            "AuthToken": access_token,
            "EffectiveDate": f"""/Date({effectivedate} +0000)/""",
            "EnableAccrualsData": True,
            "EnableCustomFieldsData": True,
            "EnableRatesData": True,
            "DataAction": {
                "Name": "SELECT-ALL",
                "Values": [""]
            }
        }
    )

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        response_code_not_200(response)
    data = response.json()
    df = pd.json_normalize(data['Results'])

    return df


def GetUserIPAccess(access_token: str, effectivedate: int) -> pd.DataFrame:
    """
    Get user basic IP Access

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    effectivedate1 : int
        Ending date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user IP Access
    """
    url = ""
    get_userIP = []
    df = GetUserBasic(access_token, effectivedate)
    for i in df['EmpIdentifier'].tolist():
        print(i)
        payload = json.dumps(
            {
                "AuthToken": access_token,
                "UserIPAccessList": [
                    {
                        "EmpIdentifier": f"""{i}""",
                        "IPMatchList": [""]
                    }
                ]
            }

        )

        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            response_code_not_200(response)
        data = response.json()
        df = pd.json_normalize(data['Results'])
        get_userIP.append(df)

    user_access = pd.concat(get_userIP, ignore_index=True)

    return user_access


def GetUserLaborLevel(access_token: str, effectivedate: int) -> pd.DataFrame:
    """
    Get user basic Labor Level

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user Labor Level
    """

    url = ""

    payload = json.dumps(
        {
            "AuthToken": access_token,
            "EffectiveDate": f"""/Date({effectivedate}+0000)/""",
            "DataAction": {
                "Name": "SELECT-ALL",
                "Values": [""]
            }
        }
    )

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        response_code_not_200(response)
    data = response.json()
    df = pd.json_normalize(data['Results'])

    return df


def GetUserLogins(access_token: str, effectivedate: int, effectivedate1: int) -> pd.DataFrame:
    """
    Get user logins

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    effectivedate1 : int
        Ending date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user logins
    """

    url = ""
    payload = json.dumps(
        {
            "AuthToken": access_token,
            "Start": "/Date(1662402600+0000)/",
            "End": "/Date(1640975400+0000)/",
            "SortOption": None,
            "DataAction": {
                "Name": "SELECT-ALL",
                "Values": [""]
            }
        }
    )

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        response_code_not_200(response)
    data = response.json()
    df = pd.json_normalize(data['Results'])

    return df


def GetUserPayAdjust(access_token: str, effectivedate: int, effectivedate1: int) -> pd.DataFrame:
    """
    Get user User Pay Adjust

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    effectivedate1 : int
        Ending date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user User Pay Adjust
    """
    url = ""

    payload = json.dumps({
        "AuthToken": access_token,
        "DateTimeSchema": 0,
        "StartDate": "/Date(1640975400+0000)/",
        "EndDate": "/Date(1662402600+0000)/",
        "DataAction": {
            "Name": "SELECT-EMPID",
            "Values": ["10057050"]
        }
    })

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        response_code_not_200(response)
    data = response.json()
    df = pd.json_normalize(data['Results'])

    return df


def GetUserSchedule(access_token: str, effectivedate: int, effectivedate1: int) -> pd.DataFrame:
    """
    Get user schedule

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    effectivedate1 : int
        Ending date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user schedule
    """
    url = ""
    get_user = []
    df = GetUserBasic(access_token, effectivedate)
    for i in df['EmpIdentifier'].tolist():
        print(i)
        payload = json.dumps(
            {
                "AuthToken": access_token,
                "StartDate": "/Date(1640975400+0000)/",
                "EndDate": "/Date(1644172200+0000)/",
                "DateTimeSchema": 0,
                "DataAction": {
                    "Name": "SELECT-ALL",
                    "Values": [f"""{i}"""]
                }
            }
        )

        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            response_code_not_200(response)
        data = response.json()
        df = pd.json_normalize(data['Results'])
        get_user.append(df)

    UserSchedule = pd.concat(get_user, ignore_index=True)

    return UserSchedule


def GetUserTimeOffBalance(access_token: str, effectivedate: int) -> pd.DataFrame:
    """
    Get user time of balance

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user time of balance
    """
    get_user = []
    df = GetUserBasic(access_token, effectivedate)
    list_identifier = [str(item) for item in df['EmpIdentifier'].tolist()]
    batch_size = 50
    batch_identifier = [list_identifier[i:i + batch_size]
                        for i in range(0, len(list_identifier), batch_size)]
    for i in batch_identifier:
        url = ""
        payload = json.dumps(
            {
                "AuthToken": access_token,
                "ProjectedBalanceDate": f"""/Date({effectivedate}+0000)/""",
                "DataAction": {
                    "Name": "SELECT-EMPID",
                    "Values": i
                }
            }
        )

        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            response_code_not_200(response)
        data = response.json()
        df = pd.json_normalize(data['Results'])
        get_user.append(df)

    df = pd.concat(get_user, ignore_index=True)

    return df


def GetUserTimeOffRequest(access_token: str, effectivedate: int, effectivedate1: int) -> pd.DataFrame:
    """
    Get user time off request

    Args
    ----
    access_token : str
        Access token to get the requests response 

    effectivedate : int
        Starting date for data pulling

    effectivedate1 : int
        Ending date for data pulling

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user time off request
    """
    url = ""

    payload = json.dumps(
        {
            "AuthToken": access_token,
            "StartDate": f"""/Date({effectivedate}+0000)/""",
            "EndDate": f"""/Date({effectivedate1}+0000)/""",
            "DateTimeSchema": 0,
            "DataAction": {
                "Name": "SELECT-ALL",
                "Values": [""]
            }
        }
    )

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        response_code_not_200(response)
    data = response.json()
    df = pd.json_normalize(data['Results'])
    return df


def get_select_all(access_token: str, endpoint: str) -> pd.DataFrame:
    """
    Get user data according to endpoint

    Args
    ----
    access_token : str
        Access token to get the requests response 

    endpoint : str
        Endpoint of ulr

    Returns
    -------
    df : pandas.DataFrame
        The dataframe of user data according to endpoint
    """
    url = f""""""

    payload = json.dumps(
        {
            "AuthToken": access_token,
            "DataAction": {
                "Name": "SELECT-ALL",
                "Values": [""]
            }
        }
    )

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        response_code_not_200(response)
    data = response.json()
    df = pd.json_normalize(data['Results'])

    return df


def main():

    effectivedate = get_date()[0]
    effectivedate1 = get_date()[1]

    access_token = get_access_token()

    conn = sq.ss_conn(sq.SQL_DB)

    # Lift and Shift
    df_User_Basic = GetUserBasic(access_token, effectivedate)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetUserBasic',
                           df_User_Basic, truncate_table=False, drop_table=True)

    endpoint = "GetCustomField"
    df = get_select_all(access_token, endpoint)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetCustomField',
                           df, truncate_table=False, drop_table=True)

    endpoint = "GetHolidayList"
    df = get_select_all(access_token, endpoint)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetHolidayList',
                           df, truncate_table=False, drop_table=True)

    endpoint = "GetHolidayListDetail"
    df = get_select_all(access_token, endpoint)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetHolidayListDetail',
                           df, truncate_table=False, drop_table=True)

    endpoint = "GetLaborLevel"
    df = get_select_all(access_token, endpoint)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetLaborLevel',
                           df, truncate_table=False, drop_table=True)

    endpoint = "GetLaborLevelDetail"
    df = get_select_all(access_token, endpoint)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetLaborLevelDetail',
                           df, truncate_table=False, drop_table=True)

    endpoint = "GetPayrollPayAdjust"
    df = get_select_all(access_token, endpoint)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetPayrollPayAdjust',
                           df, truncate_table=False, drop_table=True)

    endpoint = "GetPayrollPayType"
    df = get_select_all(access_token, endpoint)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetPayrollPayType',
                           df, truncate_table=False, drop_table=True)

    endpoint = "GetPayrollPolicies"
    df = get_select_all(access_token, endpoint)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetPayrollPolicies',
                           df, truncate_table=False, drop_table=True)

    endpoint = "GetTimeOffPolicy"
    df = get_select_all(access_token, endpoint)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetTimeOffPolicy',
                           df, truncate_table=False, drop_table=True)

    df = GetTimeOffPolicyDetailRule(access_token)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetTimeOffPolicyDetailRule',
                           df, truncate_table=False, drop_table=True)

    df = GetUserTimeOffBalance(access_token, effectivedate)
    sq.ss_df_to_table_mgmt(conn, sq.SQL_SCHEMA, 'GetUserTimeOffBalance',
                           df, truncate_table=False, drop_table=True)

    # endpoint = "GetLaborLevelModel "
    # df=get_select_all(access_token, endpoint)   # No [json] payload data was found
    # sq.ss_df_to_table_mgmt(sq.SQL_DB, sq.SQL_SCHEMA, 'GetLaborLevelModel', df, truncate_table= False, drop_table= True)

    # endpoint = "GetSSOEnabled "
    # df=get_select_all(access_token, endpoint) #temporarily unavailable
    # sq.ss_df_to_table_mgmt(sq.SQL_DB, sq.SQL_SCHEMA, 'GetUserBasic', df, truncate_table= False, drop_table= True)

    # Incremental
    df = GetUserTimeOnStatusBoard(access_token, effectivedate)
    sq.ss_incremental_data_mgmt(
        conn, df, sq.SQL_SCHEMA, "GetUserTimeOnStatusBoard", "EmpIdentifier")

    df = GetPunchPrompts(access_token, effectivedate)
    sq.ss_incremental_data_mgmt(
        conn, df, sq.SQL_SCHEMA, "GetPunchPrompts", "EmployeeIdentifier")

    df = TimeGetPunchesByEmpIdentifier(
        access_token, effectivedate, effectivedate1)
    sq.ss_incremental_data_mgmt(
        conn, df, sq.SQL_SCHEMA, "TimeGetPunchesByEmpIdentifier", "InTimeSlicePreID")

    df = TimeExport(access_token, effectivedate, effectivedate1)
    sq.ss_incremental_data_mgmt(
        conn, df, sq.SQL_SCHEMA, "TimeExport", "EmpIdentifier")

    df = GetUserDetail(access_token, effectivedate)
    sq.ss_incremental_data_mgmt(
        conn, df, sq.SQL_SCHEMA, "GetUserDetail", "Profile.UserID")

    df = GetUserLaborLevel(access_token, effectivedate)
    sq.ss_incremental_data_mgmt(
        conn, df, sq.SQL_SCHEMA, "GetUserLaborLevel", "EmpIdentifier")

    # # df=GetUserIPAccess(access_token,effectivedate)
    # # sq.ss_incremental_data_mgmt(df, "GetUserIPAccess" , "unique_id")

    # # df=GetUserLogins(access_token,effectivedate,effectivedate1)
    # # sq.ss_incremental_data_mgmt(df, "GetUserLogins" , "unique_id")

    # ##Start Date and End Date range should be with in 180 days
    # # df=GetUserPayAdjust(access_token)
    # # sq.ss_incremental_data_mgmt(df, "GetUserPayAdjust" , "unique_id")

    # ##Start Date and End Date range should be with in 45 days
    # # df=GetUserSchedule(access_token,effectivedate,effectivedate1)
    # # sq.ss_incremental_data_mgmt(df, "GetUserSchedule" , "unique_id")

    #  ##Start Date and End Date range should be with in 60 days
    # # df=GetUserTimeOffRequest(access_token,effectivedate,effectivedate1)
    # # sq.ss_incremental_data_mgmt(df, "GetUserTimeOffRequest" , "unique_id")
