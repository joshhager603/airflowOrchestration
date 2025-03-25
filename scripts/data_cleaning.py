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
    df = df.drop_duplicates(subset=['employee_id'])

    # 2. remove any rows with missing entries
    df = df.dropna()

    # 3. fix YYYY/MM/DD formatting issues, to YYYY-MM-DD
    df['date_of_joining'] = pd.to_datetime(df['date_of_joining'])
    df['date_of_joining'] = df['date_of_joining'].dt.strftime('%Y-%m-%d')

    # 4. Fix non-compliant entries in Department column
    df['department'] = df['department'].replace(DEPARTMENT_CORRECTIONS)
    manual_column_correct(df, 'department', DEPARTMENT_GROUP)

    # 5. Fix non-compliant entries in Country column
    df['country'] = df['country'].str.capitalize()
    df['country'] = df['country'].replace(COUNTRY_CORRECTIONS)
    manual_column_correct(df, 'country', COUNTRY_GROUP)

    # 6. Fix non-compliant entries in Performance Rating column
    df['performance_rating'] = df['performance_rating'].replace(DEPARTMENT_CORRECTIONS)
    manual_column_correct(df, 'performance_rating', PERFORMANCE_RATING_GROUP)

    # 7. confirm all types are correct
    df["employee_id"] = df["employee_id"].astype(int)
    df["name"] = df["name"].astype(str)
    df["age"] = df["age"].astype(int)
    df["department"] = df["department"].astype(str)
    df['date_of_joining'] = pd.to_datetime(df['date_of_joining'])
    df['years_of_experience'] = df["years_of_experience"].astype(int)
    df['country'] = df["country"].astype(str)
    df['salary'] = df["salary"].astype(int)
    df['performance_rating'] = df["performance_rating"].astype(str)

    return df