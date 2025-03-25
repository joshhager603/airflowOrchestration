from scripts import common_functions
from scripts.constants import *
import pandas as pd

DEPARTMENT_GROUP = ['Marketing', 'R&D', 'Legal', 'HR', 'IT', 'Operations', 
                    'Sales', 'Customer Support', 'Finance', 'Logistics']

COUNTRY_GROUP = ['Drivania', 'Glarastan', 'Xanthoria', 'Velronia', 'Mordalia', 
                 'Vorastria', 'Luronia', 'Tavlora', 'Zorathia', 'Hesperia']

PERFORMANCE_RATING_GROUP = ['Average Performers', 'High Performers', 
                            'Low Performers', 'Top Performers', 'Poor Performers']

DEPARTMENT_GROUP = ['Marketing', 'R&D', 'Legal', 'HR', 'IT', 'Operations', 
                    'Sales', 'Customer Support', 'Finance', 'Logistics']

DEPARTMENT_CORRECTIONS = {
    'Cust Support': 'Customer Support',
    'H R': 'HR',
    'Fin': 'Finance',
    'sales': 'Sales',
    'Hr': 'HR',
    'RnD': 'R&D',
    'It': 'IT',
    'logistics': 'Logistics',
    'customer support': 'Customer Support',
    'Finanace': 'Finance',
    'Research': 'R&D',
    'Marketng': 'Marketing',
    'Support': 'Customer Support',
    'it': 'IT',
    'CustomerSupport': 'Customer Support',
    'r&d': 'R&D',
    'Human Resources': 'HR',
    'Lgistics': 'Logistics',
    'finance': 'Finance',
    'Slaes': 'Sales',
    'Oprations': 'Operations',
    'Marketng': 'Marketing',
    'operations': 'Operations',
    'Legl': 'Legal',
    'Logstics': 'Logistics',
}

COUNTRY_CORRECTIONS = { 
    'Xanth0ria': 'Xanthoria'
}

PERFORMANCE_RATING_CORRECTIONS = { 
    'Top Performers': 'High Performers',
    'Poor Performers': 'Low Performers'
}

def manual_column_correct(df: pd.DataFrame, column_name: str, group: list[str]):
    print(f'\n ----- Manual column correct for column {column_name} -----')

    for index, row in df.iterrows():
        current_value = row[column_name]

        new_value = ""
        if current_value not in group:
            print(f"\nFound entry {current_value} that does not belong in group.")
            print(f"Group is: {group}")
            new_value = input("Enter new value: ").strip()

        if new_value:
            df.at[index, column_name] = new_value
    
    print(f"All entries in {column_name} have been checked for conformity to group.")

def clean_data() -> pd.DataFrame:

    # pull the raw data from postgres
    engine = common_functions.create_sqlalchemy_engine()
    df = pd.read_sql(f'SELECT * FROM {RAW_DATA_TABLE_NAME}', engine)

    # 1. remove duplicate entries based on Employee Id column
    df = df.drop_duplicates(subset=['Employee Id'])

    # 2. remove any rows with missing entries
    df = df.dropna()

    # 3. fix YYYY/MM/DD formatting issues, to YYYY-MM-DD
    df['Date of Joining'] = pd.to_datetime(df['Date of Joining'])
    df['Date of Joining'] = df['Date of Joining'].dt.strftime('%Y-%m-%d')

    # 4. Fix non-compliant entries in Department column
    df['Department'] = df['Department'].replace(DEPARTMENT_CORRECTIONS)
    manual_column_correct(df, 'Department', DEPARTMENT_GROUP)

    # 5. Fix non-compliant entries in Country column
    df['Country'] = df['Country'].str.capitalize()
    df['Country'] = df['Country'].replace(COUNTRY_CORRECTIONS)
    manual_column_correct(df, 'Country', COUNTRY_GROUP)

    # 6. Fix non-compliant entries in Performance Rating column
    df['Performance Rating'] = df['Performance Rating'].replace(DEPARTMENT_CORRECTIONS)
    manual_column_correct(df, 'Performance Rating', PERFORMANCE_RATING_GROUP)

    # 7. confirm all types are correct
    df["Employee Id"] = df["Employee Id"].astype(int)
    df["Name"] = df["Name"].astype(str)
    df["Age"] = df["Age"].astype(int)
    df["Department"] = df["Department"].astype(str)
    df['Date of Joining'] = pd.to_datetime(df['Date of Joining'])
    df['Years of Experience'] = df["Years of Experience"].astype(int)
    df['Country'] = df["Country"].astype(str)
    df['Salary'] = df["Salary"].astype(int)
    df['Performance Rating'] = df["Performance Rating"].astype(str)

    return df