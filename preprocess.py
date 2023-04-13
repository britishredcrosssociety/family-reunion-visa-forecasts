import pandas as pd

def load_data():
    df1 = pd.read_csv('applications.csv')
    df2 = pd.read_csv('decisions_resettlement.csv')
    df3 = pd.read_csv('family_reunion.csv')
    df1['Date'] = pd.to_datetime(df1['Date'])
    df2['Date'] = pd.to_datetime(df2['Date'])
    df3['Date'] = pd.to_datetime(df3['Date'])
    return df1, df2, df3

def merge_all(df1, df2, df3, ix = 24, column_of_interest = 'applications'):
    ''' merges 3 dataframes to establish for each country the applicaions, decisions, and no of family reunion applied
    takes the top ix countries based on application, sums the rest as other, and returns a pivoted dataframe
    for the column of interest. also outputs median average smoothed version '''
    
    columns_used = ['Date', 'Year', 'Quarter', 'Nationality', 'Region', 'Age', 'Sex',
           'Applicant type', 'UASC', 'Location of application', 'Applications']
    allgroups = ['Date', 'Year', 'Quarter','Nationality']
    df_group = df1[columns_used].groupby(allgroups).sum().reset_index()

    columns_used = ['Date', 'Year', 'Quarter', 'Nationality', 'Region', 'Age', 'Sex',
           'Applicant type', 'UASC', 'Location of application', 'Applications']
    allgroups = ['Date', 'Year', 'Quarter','Nationality']
    df_select = df1[(df1['Applicant type'] == 'Dependant')]  
    df_group_dep = df_select[columns_used].groupby(allgroups).sum().reset_index().rename(columns = {'Applications': 'Dependant applications'})

    columns_used = ['Date', 'Year', 'Quarter', 'Nationality', 'Region', 'Age', 'Visas granted']
    allgroups = ['Date', 'Year', 'Quarter','Nationality']
    df_group2 = df3[columns_used].groupby(allgroups).sum().reset_index()

    # Create a merged file of both applications and visas granted
 
    df_group_merged = df_group.merge(df_group2.drop(columns=['Year','Quarter']), on = ['Nationality','Date']).merge(df_group_dep.drop(columns=['Year','Quarter']), on = ['Nationality','Date'])
    
    da1 = df_group_merged.pivot('Date','Nationality','Applications').fillna(0)
    
    dg0  = df_group_merged.groupby('Nationality').sum()[['Applications']].sort_values('Applications',ascending=False)
    top_countries_new = dg0.reset_index()[:ix]['Nationality'].values
    other_countries_new = dg0.reset_index()[ix:]['Nationality'].values

    out = da1[top_countries_new].copy()
    out['Other'] = da1[other_countries_new].sum(axis=1)
    
    # smoothed
    out_sm = out.rolling(3).median().shift(-2)
    out_sm = out.rolling(5).median().shift(-2).bfill().dropna()
    
    return df_group_merged, out, out_sm,  top_countries_new