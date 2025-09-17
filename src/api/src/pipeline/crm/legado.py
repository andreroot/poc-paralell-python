# def get_loadPhysicalAssets():
    
#     df_final = pd.DataFrame()
    
#     try:
#         print(f'Buscando Informações do id: loadPhysicalAssets')
#         url = f"https://api.novo.thunders.com.br/gw/crm/api/loadPhysicalAssets"
#         headers = {
#             'authorization': 'Bearer '+ get_token()
#         }
#         response = requests.request("GET", url, headers=headers)
#         data_json = response.json()
#         df = pd.json_normalize(data_json)
        
#         df_final = pd.concat([df_final, df], axis=0)
        
#     except Exception as e:
#         print(e)
#     df_final.reset_index(drop=True, inplace=True)
    
#     return df_final

# def get_loadPhysicalAssets_details(df):
    
#     df_final = pd.DataFrame()
    
#     for id in df['id'].unique().tolist():
        
#         try:
#             print(f'Buscando Informações do id: {id}')

#             url = f"https://api.novo.thunders.com.br/gw/crm/api/loadPhysicalAssets/{id}"

#             headers = {
#                 'authorization': 'Bearer '+ get_token()
#             }

#             response = requests.request("GET", url, headers=headers)
#             data_json = response.json()
#             df = pd.json_normalize(data_json)
            
#             df_final = pd.concat([df_final, df], axis=0)
            
#         except Exception as e:
#             print(e)
#         df_final.reset_index(drop=True, inplace=True)
    
#     return df_final

# def get_demands(df):
#     # Create an empty list to hold all the expanded demand data
#     all_demands_data = []
    
#     # Iterate through each row in the DataFrame
#     for index, row in df.iterrows():
#         if row['demands']:  # Check if 'demands' column contains data
#             demands_list = row['demands']
            
#             # If it's already a list, process it
#             for demand in demands_list:
#                 # Add the original id and name to each demand row
#                 print(row['id'],row['name'])
#                 demand['parent_id'] = row['id']
#                 demand['parent_name'] = row['name']
                
#                 # Append the demand to the list of all demand data
#                 all_demands_data.append(demand)
    
#     # Convert the list of demands to a new DataFrame
#     demands_df = json_normalize(all_demands_data)
    
#     # Display the resulting DataFrame
#     return demands_df

# df_loadPhysicalAssets = get_loadPhysicalAssets()
# df_loadPhysicalAssets_details = get_loadPhysicalAssets_details(df_loadPhysicalAssets)
# df_demands = get_demands(df_loadPhysicalAssets_details)
# df_demands


# # df_api_MeasuringProjectionConsolidateMonthYear
# forced_dtype = {
#  'demands': types.Text,
#     'economySinceAcl': types.String(length=255),
#     'useFiveMinutesReading': types.Boolean,
#     'industrializationPercentagePeriods': types.String(length=255),
#     'id': types.String(length=255),
#     'name': types.String(length=255),
#     'hasSameCompanyAddress': types.Boolean,
#     'connection': types.String(length=255),
#     'parcels': types.String(length=255),
#     'covidTariffDateRequest': types.String(length=255),
#     'generator.enabled': types.Boolean,
#     'generator.periods': types.String(length=255),
#     'company.id': types.String(length=255),
#     'company.name': types.String(length=255),
#     'company.alias': types.String(length=255),
#     'company.isDeleted': types.Boolean,
#     'address.street': types.String(length=255),
#     'address.number': types.String(length=255),
#     'address.complement': types.String(length=255),
#     'address.neighborhood': types.String(length=255),
#     'address.city': types.String(length=255),
#     'address.state': types.String(length=255),
#     'address.zipcode': types.String(length=255),
#     'connectionType.id': types.Integer,
#     'connectionType.name': types.String(length=255),
#     'connection.id': types.String(length=255),
#     'connection.name': types.String(length=255),
#     'connection.initials': types.String(length=255)
# }
# #convert_column_to_datetime(loadPhysicalAssets_details, 'startDate')
# #convert_column_to_datetime(loadPhysicalAssets_details, 'endDate')

# # Columns to remove
# #columns_to_remove = ['testPeriods', 'associatedNetworks',	'associatedAssets']

# # Convert list columns to JSON strings
# columns_with_lists = ['demands', 'industrializationPercentagePeriods', 'parcels', 'generator.periods']  # Replace with your column names
# df_cleaned = convert_lists_to_text(df_loadPhysicalAssets_details, columns_with_lists)

# # Remove columns
# #df_cleaned = remove_columns(df_MeasuringPoint_details, columns_to_remove)
# # Assuming df_final is your DataFrame
# send_df_to_sql_with_schema(df_cleaned, 'lkok', 'loadPhysicalAssets_details', server, database, forced_dtype)